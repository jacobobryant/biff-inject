(ns com.biffweb.graph
  "A lightweight implementation of pathom-style resolvers.

  Supports:
  - Simple resolvers with declared input/output
  - Nested queries (joins)
  - Nested inputs (resolvers that require sub-attributes of their inputs)
  - Optional inputs ([:? :key] syntax)
  - Optional query items ([:? :key] in query vectors)
  - Global resolvers (no input)
  - Var-based resolvers (metadata-driven)
  - Batch resolvers (process multiple entities at once, breadth-first)
  - Per-query resolver caching (avoids redundant resolver calls)
  - Strict mode only (throws on missing data)

  Omits (compared to pathom3):
  - Plugin system
  - Lenient mode
  - Query planning (uses query directly)
  - EQL AST manipulation"
  (:require [com.biffweb.fx :as fx]))

;; ---------------------------------------------------------------------------
;; Input helpers
;; ---------------------------------------------------------------------------

(defn- optional-input?
  "Returns true if input-item is an optional input marker [:? ...]."
  [input-item]
  (and (vector? input-item)
       (= :? (first input-item))))

(defn- unwrap-optional
  "Given an optional input marker [:? x], returns x."
  [input-item]
  (second input-item))

(defn- input-item-key
  "Extract the top-level key from an input item (keyword, join map, or optional wrapper)."
  [input-item]
  (cond
    (optional-input? input-item) (input-item-key (unwrap-optional input-item))
    (map? input-item) (let [k (ffirst input-item)]
                        (if (optional-input? k)
                          (input-item-key (unwrap-optional k))
                          k))
    :else input-item))

(defn- input-item-optional?
  "Returns true if the input item is optional (either [:? :key] or {[:? :key] [...]})."
  [input-item]
  (or (optional-input? input-item)
      (and (map? input-item)
           (optional-input? (ffirst input-item)))))

(defn- normalize-input-item
  "Unwrap optional markers from an input item for processing.
  E.g. [:? :foo] -> :foo, {[:? :bar] [...]} -> {:bar [...]}"
  [input-item]
  (cond
    (optional-input? input-item) (unwrap-optional input-item)
    (and (map? input-item) (optional-input? (ffirst input-item)))
    {(unwrap-optional (ffirst input-item)) (val (first input-item))}
    :else input-item))

;; ---------------------------------------------------------------------------
;; Registry helpers
;; ---------------------------------------------------------------------------

(defn resolver
  "Define a resolver. Accepts either a map or a var.

  When given a var:
  - Uses var metadata for :input, :output, and :batch
  - Derives :id from the var's namespace and name
  - Stores the var itself (not the deref'd fn) as :resolve

  When given a map, expects:
    :id      - keyword, unique resolver id
    :input   - vector of input specs (keywords, join maps, or optional wrappers)
    :output  - vector of flat keywords this resolver provides
    :resolve - (fn [ctx input-map] output-map), or if :batch is true,
               (fn [ctx [input-map ...]] [output-map ...])
    :batch   - (optional) if true, the resolver takes a vector of input maps
               and returns a vector of output maps in the same order

  Returns a resolver map with keys :id, :input, :output, :resolve, :batch."
  [resolver-or-map]
  (if (var? resolver-or-map)
    (let [var-meta (meta resolver-or-map)
          id (keyword (str (:ns var-meta)) (str (:name var-meta)))
          input (or (:input var-meta) [])
          output (or (:output var-meta) [])
          batch (boolean (:batch var-meta))]
      (when (some map? output)
        (throw (ex-info "Resolver :output must be flat keywords, not nested maps"
                        {:resolver id :output output})))
      {:id id
       :input input
       :output output
       :resolve resolver-or-map
       :batch batch})
    (let [{:keys [id input output resolve batch]} resolver-or-map]
      (when-not id
        (throw (ex-info "Resolver must have an :id" {:resolver resolver-or-map})))
      (when-not resolve
        (throw (ex-info "Resolver must have a :resolve function" {:resolver resolver-or-map})))
      (when (some map? output)
        (throw (ex-info "Resolver :output must be flat keywords, not nested maps"
                        {:resolver id :output output})))
      {:id id
       :input (or input [])
       :output (or output [])
       :resolve resolve
       :batch (boolean batch)})))

;; ---------------------------------------------------------------------------
;; Caching wrappers
;; ---------------------------------------------------------------------------

(defn- wrap-caching
  "Wrap a resolver's :resolve function with per-query caching.
  The cache is an atom stored in ctx under :biff.graph/cache.
  For non-batch resolvers, acts like memoize keyed on the input map.
  For batch resolvers, checks each input element individually and only
  sends unique uncached inputs to the underlying resolver."
  [{:keys [id batch resolve]}]
  (if batch
    (fn [ctx inputs]
      (if-let [cache (:biff.graph/cache ctx)]
        (let [resolver-cache (get @cache id {})
              uncached-idxs (vec (keep (fn [i] (when-not (contains? resolver-cache (nth inputs i)) i))
                                       (range (count inputs))))
              ;; Deduplicate uncached inputs while preserving order
              unique-uncached (vec (distinct (map #(nth inputs %) uncached-idxs)))]
          (if (empty? unique-uncached)
            (mapv #(get resolver-cache %) inputs)
            (let [new-results (resolve ctx unique-uncached)
                  _ (swap! cache update id
                           (fn [m]
                             (reduce (fn [m [input result]]
                                       (assoc m input result))
                                     (or m {})
                                     (map vector unique-uncached new-results))))
                  updated-cache (get @cache id)]
              (mapv #(get updated-cache %) inputs))))
        (resolve ctx inputs)))
    (fn [ctx input]
      (if-let [cache (:biff.graph/cache ctx)]
        (let [resolver-cache (get @cache id)]
          (if (and resolver-cache (contains? resolver-cache input))
            (get resolver-cache input)
            (let [result (resolve ctx input)]
              (swap! cache assoc-in [id input] result)
              result)))
        (resolve ctx input)))))

(defn build-index
  "Build an index from a collection of resolvers (maps or vars).
  Calls `resolver` on each item and wraps each resolver's :resolve function
  with caching logic. When a cache atom is present in the query context
  (under :biff.graph/cache), resolved results are memoized per input.

  Accepts optional keyword arguments:
    :middleware - a vector of functions (fn [resolver-map] -> resolver-map)
                  applied after resolvers are converted to maps but before
                  building the :resolvers-by-output index.

  Returns a map with:
    :resolvers-by-output  {attr-key [resolver ...]}
    :all-resolvers        [resolver ...]"
  [resolvers & {:keys [middleware]}]
  (let [resolvers (mapv resolver resolvers)
        resolvers (reduce (fn [rs mw] (mapv mw rs))
                          resolvers
                          middleware)
        resolvers (mapv (fn [r] (assoc r :resolve (wrap-caching r)))
                        resolvers)]
    {:resolvers-by-output
     (reduce (fn [idx r]
               (reduce (fn [idx k]
                         (update idx k (fnil conj []) r))
                       idx
                       (:output r)))
              {}
              resolvers)
      :all-resolvers resolvers}))

(def ^:private index-for-modules
  (memoize
   (fn [modules]
      (let [middleware (not-empty (vec (mapcat :biff.graph/middleware modules)))]
        (apply build-index
               (mapcat :biff.graph/resolvers modules)
               (cond-> []
                 middleware (conj :middleware middleware)))))))

(declare query)

(def fx-handlers
  {:biff.graph.fx/query
   (fn [ctx & args]
     (apply query ctx args))})

(defn module
  []
  {:biff.core/init
   (fn [modules-var]
      {:biff.graph/get-index
       (fn []
         (index-for-modules @modules-var))
       :biff.fx/handlers fx-handlers})})

(defmacro defresolver
  "Defines a resolver backed by a biff.fx machine.

   The first form after the parameter vector is the main resolver body.
   Remaining keyword/fn pairs (if any) define additional machine states."
  {:arglists '([sym opts-map [ctx input] body & states])}
  [sym opts & args]
  (let [{:keys [input output]} opts
        [params body & state-kvs] args
        machine-name (keyword (str *ns*) (str sym))
        ctx-sym (first params)
        input-sym (second params)]
    `(let [start-fn# (fn [~ctx-sym ~input-sym] ~body)
           machine# (fx/machine ~machine-name
                      :start (fn [{resolver-input# :biff.fx/resolver-input :as ctx#}]
                               (start-fn# ctx# resolver-input#))
                      ~@state-kvs)]
       (defn ~sym
         ~(merge (when (seq input) {:input input})
                 {:output output})
         [ctx# input#]
         (machine# (assoc ctx# :biff.fx/resolver-input input#))))))

;; ---------------------------------------------------------------------------
;; Query engine
;; ---------------------------------------------------------------------------

(declare ^:private process-entities)

(defn- find-resolver-candidates
  "Find all resolvers that can provide `attr`."
  [ctx attr]
  (get-in (or (:biff.graph/index ctx)
              (when-some [get-index (:biff.graph/get-index ctx)]
                (get-index)))
          [:resolvers-by-output attr]))

(defn- ensure-join-value
  "Validate that a value is suitable for a join (map or sequential of maps).
  Throws if the value is nil or a scalar."
  [v attr context]
  (when (or (nil? v) (not (or (map? v) (sequential? v))))
    (throw (ex-info (str "Expected a map or collection for join on " attr
                         ", but got: " (pr-str v))
                    {:attr attr :value v :context context}))))

(defn- normalize-query-item
  "Unwrap optional markers from a query item.
  Returns [attr sub-query optional?]."
  [query-item]
  (cond
    ;; [:? :keyword] or [:? {:keyword [...]}]
    (optional-input? query-item)
    (let [inner (unwrap-optional query-item)
          [attr sub-query _] (normalize-query-item inner)]
      [attr sub-query true])

    ;; {[:? :keyword] [...]} — optional join
    (and (map? query-item) (optional-input? (ffirst query-item)))
    [(unwrap-optional (ffirst query-item)) (val (first query-item)) true]

    ;; {:keyword [...]} — regular join
    (map? query-item)
    [(ffirst query-item) (val (first query-item)) false]

    ;; :keyword — plain attribute
    :else
    [query-item nil false]))

;; ---------------------------------------------------------------------------
;; Sentinel helpers
;; ---------------------------------------------------------------------------

(defn- unresolved-result
  "Create a sentinel indicating an entity couldn't satisfy a required attribute."
  [attr entity]
  {::unresolved-entity true
   ::failed-attr attr
   ::available-keys (vec (keys entity))})

(defn- unresolved-result?
  "Check if a result is an unresolved-entity sentinel."
  [v]
  (and (map? v) (::unresolved-entity v)))

;; ---------------------------------------------------------------------------
;; Breadth-first batch processing
;; ---------------------------------------------------------------------------

(defn- resolve-attrs-batch
  "Resolve a single attr for multiple entities, trying resolver candidates in order.
  Never throws for resolution failures — returns ::unresolved for values that
  cannot be resolved. The resolving set tracks attrs for cycle detection."
  [ctx entities attr resolving]
  (if (contains? resolving attr)
    (vec (repeat (count entities) ::unresolved))
    (let [resolving' (conj resolving attr)
          candidates (find-resolver-candidates ctx attr)
          init-values (mapv (fn [e] (if (contains? e attr) (get e attr) ::unresolved)) entities)]
      (loop [values init-values
             candidates (seq candidates)]
        (let [unresolved-idxs (vec (keep-indexed (fn [i v] (when (= v ::unresolved) i)) values))]
          (if (or (empty? unresolved-idxs) (nil? candidates))
            values
            (let [r (first candidates)
                  unresolved-entities (mapv #(nth entities %) unresolved-idxs)
                  ;; Resolve inputs via process-entities with the original input query.
                  ;; Entities that can't satisfy required inputs come back as sentinels.
                  resolved-inputs (process-entities ctx unresolved-entities (:input r) resolving')
                  valid-mask (mapv #(not (unresolved-result? %)) resolved-inputs)
                  valid-inputs (vec (keep-indexed (fn [i m] (when (nth valid-mask i) m))
                                                  resolved-inputs))
                  valid-global-idxs (vec (keep-indexed
                                          (fn [i valid?]
                                            (when valid? (nth unresolved-idxs i)))
                                          valid-mask))]
              (if (empty? valid-inputs)
                (recur values (next candidates))
                (let [results (if (:batch r)
                                ((:resolve r) ctx valid-inputs)
                                (mapv #((:resolve r) ctx %) valid-inputs))
                      new-values (reduce
                                  (fn [vals [global-idx result]]
                                    (if (contains? result attr)
                                      (assoc vals global-idx (get result attr))
                                      vals))
                                  values
                                  (map vector valid-global-idxs results))]
                  (recur new-values (next candidates)))))))))))

(defn- process-entities
  "Process a query against multiple entities using breadth-first traversal.
  Always returns a vector of results, never throws for resolution failures.
  Each result is either a map of resolved attributes or an unresolved-result
  sentinel indicating the entity couldn't satisfy a required attribute.
  For optional query items, unresolved values are silently omitted.
  For required query items, unresolved values cause the entity result to become
  a sentinel. The resolving set is passed through for input resolution (cycle
  detection) and reset to #{} for sub-queries (new resolution context)."
  [ctx entities query-vec resolving]
  (if (empty? entities)
    []
    (reduce
     (fn [results query-item]
       (let [[attr sub-query optional?] (normalize-query-item query-item)
             enriched (mapv (fn [e r]
                              (if (unresolved-result? r) e (merge e r)))
                            entities results)
             values (resolve-attrs-batch ctx enriched attr resolving)]
         (if sub-query
           ;; Join: collect all children across all parents, process recursively, reassemble
           (let [child-info
                 (mapv (fn [v r enriched-ent]
                         (cond
                           (unresolved-result? r)  {:type :already-failed}
                           (= v ::unresolved)      {:type :unresolved}
                           (map? v)                {:type :map :value v}
                           (sequential? v)         {:type :seq :value v :count (count v)}
                           :else                   (do (ensure-join-value v attr :query)
                                                       {:type :invalid})))
                       values results enriched)
                 all-children
                 (into []
                       (mapcat (fn [{:keys [type value]}]
                                 (case type
                                   :map [value]
                                   :seq value
                                   [])))
                       child-info)
                 ;; Sub-queries start with fresh resolving context
                 processed (if (seq all-children)
                             (vec (process-entities ctx all-children sub-query #{}))
                             [])]
             (loop [rs results
                    idx 0
                    offset 0]
               (if (>= idx (count results))
                 rs
                 (let [{:keys [type]} (nth child-info idx)
                       r (nth rs idx)]
                   (case type
                     :already-failed
                     (recur rs (inc idx) offset)

                     :unresolved
                     (if optional?
                       (recur rs (inc idx) offset)
                       (recur (assoc rs idx (unresolved-result attr (nth enriched idx)))
                              (inc idx) offset))

                     :map
                     (let [child (nth processed offset)]
                       (if (unresolved-result? child)
                         (recur (assoc rs idx child) (inc idx) (inc offset))
                         (recur (assoc rs idx (assoc r attr child))
                                (inc idx) (inc offset))))

                     :seq
                     (let [n (:count (nth child-info idx))
                           children (subvec processed offset (+ offset n))
                           sentinel (first (filter unresolved-result? children))]
                       (if sentinel
                         (recur (assoc rs idx sentinel) (inc idx) (+ offset n))
                         (recur (assoc rs idx (assoc r attr children))
                                (inc idx) (+ offset n)))))))))
           ;; No sub-query: store values, mark entities as sentinels if required attr missing
           (mapv (fn [result v enriched-ent]
                   (cond
                     (unresolved-result? result) result
                     (= v ::unresolved)
                     (if optional? result (unresolved-result attr enriched-ent))
                     :else (assoc result attr v)))
                 results values enriched))))
     (vec (repeat (count entities) {}))
     query-vec)))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn query
  "Run an EQL query using the provided resolver index.

  Arguments:
    ctx    - context map; must include :biff.graph/index (from build-index).
             Any other keys are passed through to resolver functions.
    entity - (optional) initial entity map with seed data, or a vector of entity
             maps for batch querying. Defaults to {} if omitted.
    query  - EQL query vector, e.g. [:user/name {:user/friends [:user/name]}]
             Supports optional items via [:? :attr] syntax.

  Returns a map satisfying the query when given a single entity,
  or a vector of maps when given a vector of entities.
  Throws ExceptionInfo if any required attribute cannot be resolved."
  ([ctx query-vec]
   (query ctx {} query-vec))
  ([{:keys [biff.graph/index biff.graph/get-index] :as ctx} entity-or-entities query-vec]
   (let [index (or index
                   (when get-index
                     (get-index)))
         ctx (assoc ctx
                    :biff.graph/index index
                    :biff.graph/cache (atom {}))
          is-vec? (sequential? entity-or-entities)
          entities (if is-vec? (vec entity-or-entities) [(or entity-or-entities {})])
          results (process-entities ctx entities query-vec #{})]
      (doseq [r results]
        (when (unresolved-result? r)
          (throw (ex-info (str "No resolver found for attribute " (::failed-attr r)
                               " with available inputs " (::available-keys r))
                          {::resolve-error true
                           :attr (::failed-attr r)
                           :available-keys (::available-keys r)}))))
      (if is-vec? results (first results)))))
