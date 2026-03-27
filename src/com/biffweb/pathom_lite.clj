(ns com.biffweb.pathom-lite
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
  - Strict mode only (throws on missing data)

  Omits (compared to pathom3):
  - Plugin system
  - Lenient mode
  - Query planning (uses query directly)
  - EQL AST manipulation")

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

(defn build-index
  "Build an index from a collection of resolvers (maps or vars).
  Calls `resolver` on each item.
  Returns a map with:
    :resolvers-by-output  {attr-key [resolver ...]}
    :all-resolvers        [resolver ...]"
  [resolvers]
  (let [resolvers (mapv resolver resolvers)]
    {:resolvers-by-output
     (reduce (fn [idx r]
               (reduce (fn [idx k]
                         (update idx k (fnil conj []) r))
                       idx
                       (:output r)))
             {}
             resolvers)
     :all-resolvers resolvers}))

;; ---------------------------------------------------------------------------
;; Query engine
;; ---------------------------------------------------------------------------

(declare ^:private process-entities)

(defn- find-resolver-candidates
  "Find all resolvers that can provide `attr`."
  [ctx attr]
  (get-in (:biff.pathom-lite/index ctx) [:resolvers-by-output attr]))

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

(defn- make-input-optional
  "Wrap an input item in [:? ...] if it's not already optional."
  [input-item]
  (if (input-item-optional? input-item)
    input-item
    [:? input-item]))

;; ---------------------------------------------------------------------------
;; Breadth-first batch processing
;; ---------------------------------------------------------------------------

(defn- resolve-attrs-batch
  "Resolve a single attr for multiple entities, trying resolver candidates in order.
  Uses batch resolvers when available; non-batch resolvers are called individually.
  Input resolution uses process-entities for batch input resolution.
  Per-entity failures are handled individually: entities that can't satisfy a
  resolver's required inputs are skipped and tried with subsequent candidates.
  The resolving set tracks attrs being transitively resolved for cycle detection."
  [ctx entities attr optional? resolving]
  (if (contains? resolving attr)
    (if optional?
      (vec (repeat (count entities) ::unresolved))
      (throw (ex-info (str "Cycle detected while resolving " attr)
                      {::resolve-error true :attr attr :resolving resolving})))
    (let [resolving' (conj resolving attr)
          candidates (find-resolver-candidates ctx attr)
          init-values (mapv (fn [e] (if (contains? e attr) (get e attr) ::unresolved)) entities)]
      (loop [values init-values
             candidates (seq candidates)]
        (let [unresolved-idxs (vec (keep-indexed (fn [i v] (when (= v ::unresolved) i)) values))]
          (if (or (empty? unresolved-idxs) (nil? candidates))
            ;; Done: check for remaining unresolved
            (if optional?
              values
              (let [first-unresolved (first (keep-indexed (fn [i v] (when (= v ::unresolved) i)) values))]
                (if first-unresolved
                  (throw (ex-info (str "No resolver found for attribute " attr
                                       " with available inputs " (keys (nth entities first-unresolved)))
                                  {::resolve-error true :attr attr
                                   :available-keys (keys (nth entities first-unresolved))}))
                  values)))
            ;; Try next candidate
            (let [r (first candidates)
                  unresolved-entities (mapv #(nth entities %) unresolved-idxs)
                  ;; Resolve inputs using process-entities with all-optional query
                  ;; so per-entity failures don't throw
                  optional-input-query (mapv make-input-optional (:input r))
                  resolved-inputs (process-entities ctx unresolved-entities optional-input-query resolving')
                  ;; Determine which entities have all required inputs
                  required-keys (set (keep (fn [item]
                                             (when-not (input-item-optional? item)
                                               (input-item-key item)))
                                           (:input r)))
                  valid-mask (mapv (fn [m] (every? #(contains? m %) required-keys))
                                   resolved-inputs)
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
  For each query item, resolves it across ALL entities before moving to the next.
  For joins with sequential values, flattens all children across all parents
  and processes them in a single recursive call. This means a batch resolver
  at depth N is called exactly once for all entities at that depth, regardless
  of how many parents exist at depth N-1.
  The resolving set is passed through for input resolution (cycle detection)
  and reset to #{} for sub-queries (new resolution context for child entities)."
  [ctx entities query-vec resolving]
  (if (empty? entities)
    []
    (reduce
     (fn [results query-item]
       (let [[attr sub-query optional?] (normalize-query-item query-item)
             enriched (mapv merge entities results)
             values (resolve-attrs-batch ctx enriched attr optional? resolving)]
         (if sub-query
           ;; Join: collect all children across all parents, process recursively, reassemble
           (let [child-types (mapv (fn [v]
                                     (cond
                                       (= v ::unresolved)  :unresolved
                                       (map? v)            :map
                                       (sequential? v)     :seq
                                       :else               (do (ensure-join-value v attr :query)
                                                               :invalid)))
                                   values)
                 ;; Flatten all children into a single list for batch processing
                 all-children (into []
                                    (mapcat (fn [v t]
                                              (case t
                                                :map [v]
                                                :seq v
                                                []))
                                            values child-types))
                 ;; Sub-queries start with fresh resolving context
                 processed (if (seq all-children)
                             (vec (process-entities ctx all-children sub-query #{}))
                             [])
                 ;; Reassemble results back into parent structure
                 final (loop [rs (transient results)
                              idx 0
                              offset 0]
                         (if (>= idx (count results))
                           (persistent! rs)
                           (let [t (nth child-types idx)]
                             (case t
                               :unresolved
                               (recur rs (inc idx) offset)

                               :map
                               (recur (assoc! rs idx (assoc (nth results idx) attr (nth processed offset)))
                                      (inc idx)
                                      (inc offset))

                               :seq
                               (let [n (count (nth values idx))
                                     children (subvec processed offset (+ offset n))]
                                 (recur (assoc! rs idx (assoc (nth results idx) attr children))
                                        (inc idx)
                                        (+ offset n)))))))]
            final)
           ;; No sub-query: store values directly
           (mapv (fn [result v]
                   (if (= v ::unresolved)
                     result
                     (assoc result attr v)))
                 results values))))
     (vec (repeat (count entities) {}))
     query-vec)))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn query
  "Run an EQL query using the provided resolver index.

  Arguments:
    ctx    - context map; must include :biff.pathom-lite/index (from build-index).
             Any other keys are passed through to resolver functions.
    entity - initial entity map with seed data (or {}), or a vector of entity maps
             for batch querying.
    query  - EQL query vector, e.g. [:user/name {:user/friends [:user/name]}]
             Supports optional items via [:? :attr] syntax.

  Returns a map satisfying the query when given a single entity,
  or a vector of maps when given a vector of entities."
  [{:keys [biff.pathom-lite/index] :as ctx} entity-or-entities query-vec]
  (if (sequential? entity-or-entities)
    (process-entities ctx (vec entity-or-entities) query-vec #{})
    (first (process-entities ctx [(or entity-or-entities {})] query-vec #{}))))
