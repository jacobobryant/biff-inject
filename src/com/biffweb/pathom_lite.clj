(ns com.biffweb.pathom-lite
  "A lightweight implementation of pathom-style resolvers.

  Supports:
  - Simple resolvers with declared input/output
  - Nested queries (joins)
  - Nested inputs (resolvers that require sub-attributes of their inputs)
  - Global resolvers (no input)
  - Strict mode only (throws on missing data)

  Omits (compared to pathom3):
  - Plugin system
  - Lenient mode
  - Batch resolvers
  - Query planning (uses query directly)
  - EQL AST manipulation
  - Optional resolvers")

;; ---------------------------------------------------------------------------
;; Registry helpers
;; ---------------------------------------------------------------------------

(defn resolver
  "Define a resolver. Returns a resolver map.

  Options:
    :name     - keyword, unique resolver name
    :input    - vector of keywords this resolver requires
    :output   - vector of flat keywords this resolver provides
    :resolve  - (fn [env input-map] output-map)"
  [{:keys [name input output resolve] :as opts}]
  (when-not name
    (throw (ex-info "Resolver must have a :name" {:resolver opts})))
  (when-not resolve
    (throw (ex-info "Resolver must have a :resolve function" {:resolver opts})))
  (when (some map? output)
    (throw (ex-info "Resolver :output must be flat keywords, not nested maps"
                    {:resolver name :output output})))
  (assoc opts
         :input  (or input [])
         :output (or output [])))

(defn build-index
  "Build an index from a collection of resolvers.
  Returns a map with:
    :resolvers-by-output  {attr-key [resolver ...]}
    :all-resolvers        [resolver ...]"
  [resolvers]
  (let [resolvers (mapv (fn [r] (if (:resolve r) r (resolver r))) resolvers)]
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

(declare process-query)
(declare ^:private resolve-attr)

(defn- find-resolver-candidates
  "Find all resolvers that can provide `attr`."
  [index attr]
  (get-in index [:resolvers-by-output attr]))

(defn- input-item-key
  "Extract the top-level key from an input item (keyword or join map)."
  [input-item]
  (if (map? input-item) (ffirst input-item) input-item))

(defn- ensure-join-value
  "Validate that a value is suitable for a join (map or sequential of maps).
  Throws if the value is nil or a scalar."
  [v attr context]
  (when (or (nil? v) (not (or (map? v) (sequential? v))))
    (throw (ex-info (str "Expected a map or collection for join on " attr
                         ", but got: " (pr-str v))
                    {:attr attr :value v :context context}))))

(defn- resolve-input-map
  "Resolve all required inputs and build the (possibly nested) input map.
  Handles both flat keyword inputs and nested join inputs like
  [{:order/user [:user/name]}]."
  [env index entity input resolving]
  ;; First pass: ensure all top-level keys are resolved in the entity
  (let [enriched (reduce
                  (fn [ent input-item]
                    (let [k (input-item-key input-item)]
                      (if (contains? ent k)
                        ent
                        (assoc ent k (resolve-attr env index ent k nil resolving)))))
                  entity
                  input)]
    ;; Second pass: build the input map with proper nesting
    (reduce
     (fn [result input-item]
       (if (map? input-item)
         (let [attr      (ffirst input-item)
               sub-input (val (first input-item))
               v         (get enriched attr)]
           (ensure-join-value v attr :input)
           (assoc result attr
                  (if (map? v)
                    (resolve-input-map env index v sub-input resolving)
                    (mapv #(resolve-input-map env index % sub-input resolving) v))))
         (assoc result input-item (get enriched input-item))))
     {}
     input)))

(defn- resolve-attr
  "Resolve a single attribute (possibly a join) for the given entity.
  `resolving` is a set of attrs currently being resolved (cycle detection)."
  [env index entity attr sub-query resolving]
  (if (contains? entity attr)
    ;; Already present — but if there's a sub-query we need to process children
    (let [v (get entity attr)]
      (if sub-query
        (do
          (ensure-join-value v attr :query)
          (if (map? v)
            (process-query env index v sub-query)
            (mapv #(process-query env index % sub-query) v)))
        v))
    ;; Cycle detection
    (if (contains? resolving attr)
      (throw (ex-info (str "Cycle detected while resolving " attr)
                      {:attr attr :resolving resolving}))
      (let [resolving (conj resolving attr)
            candidates (find-resolver-candidates index attr)
            ;; Try each candidate; pick the first one whose return contains the key
            resolved (some
                      (fn [r]
                        (try
                          (let [input-map (resolve-input-map env index entity (:input r) resolving)
                                result ((:resolve r) env input-map)]
                            (when (contains? result attr)
                              {:value (get result attr)}))
                          (catch clojure.lang.ExceptionInfo _e
                            nil)))
                      candidates)]
        (if resolved
          (let [v (:value resolved)]
            (if sub-query
              (do
                (ensure-join-value v attr :query)
                (if (map? v)
                  (process-query env index v sub-query)
                  (mapv #(process-query env index % sub-query) v)))
              v))
          (throw (ex-info (str "No resolver found for attribute " attr
                               " with available inputs " (keys entity))
                          {:attr attr :available-keys (keys entity)})))))))

(defn- process-query
  "Process an EQL query against the given entity using the resolver index.
  Returns a map of the requested attributes."
  [env index entity query]
  (reduce
   (fn [result query-item]
     (let [[attr sub-query] (if (map? query-item)
                              [(ffirst query-item) (val (first query-item))]
                              [query-item nil])
           ;; Merge already-resolved data into entity so subsequent resolvers
           ;; can use attributes resolved earlier in this query.
           enriched-entity (merge entity result)
           v (resolve-attr env index enriched-entity attr sub-query #{})]
       (assoc result attr v)))
   {}
   query))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn process
  "Run an EQL query using the provided resolvers.

  Options:
    :resolvers - collection of resolver maps (from `resolver`)
    :query     - EQL query vector, e.g. [:user/name {:user/friends [:friend/name]}]
    :entity    - (optional) initial entity map with seed data
    :env       - (optional) environment map passed to resolver fns

  Returns a map satisfying the query."
  [{:keys [resolvers query entity env]}]
  (let [index (build-index resolvers)
        entity (or entity {})]
    (process-query (or env {}) index entity query)))
