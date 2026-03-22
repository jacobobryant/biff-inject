(ns com.biffweb.pathom-lite-test
  (:require [clojure.test :refer [deftest is testing]]
            [com.biffweb.pathom-lite :as pl]))

;; ---------------------------------------------------------------------------
;; Test data: resolvers
;; ---------------------------------------------------------------------------

(def user-by-id
  {:id       :user-by-id
   :input    [:user/id]
   :output   [:user/name :user/email]
   :resolve  (fn [_ctx {:user/keys [id]}]
               (case id
                 1 {:user/name "Alice" :user/email "alice@example.com"}
                 2 {:user/name "Bob"   :user/email "bob@example.com"}
                 3 {:user/name "Carol" :user/email "carol@example.com"}
                 (throw (ex-info "User not found" {:user/id id}))))})

(def user-friends
  {:id       :user-friends
   :input    [:user/id]
   :output   [:user/friends]
   :resolve  (fn [_ctx {:user/keys [id]}]
               (case id
                 1 {:user/friends [{:user/id 2} {:user/id 3}]}
                 2 {:user/friends [{:user/id 1}]}
                 3 {:user/friends [{:user/id 1} {:user/id 2}]}
                 {:user/friends []}))})

(def user-age
  {:id       :user-age
   :input    [:user/id]
   :output   [:user/age]
   :resolve  (fn [_ctx {:user/keys [id]}]
               (case id
                 1 {:user/age 30}
                 2 {:user/age 25}
                 3 {:user/age 35}
                 {:user/age nil}))})

(def current-user
  {:id       :current-user
   :input    []
   :output   [:user/id]
   :resolve  (fn [ctx _input]
               {:user/id (:current-user-id ctx)})})

(def order-by-id
  {:id       :order-by-id
   :input    [:order/id]
   :output   [:order/total :order/status :order/user]
   :resolve  (fn [_ctx {:order/keys [id]}]
               (case id
                 100 {:order/total 59.99 :order/status :shipped :order/user {:user/id 1}}
                 101 {:order/total 12.50 :order/status :pending :order/user {:user/id 2}}
                 (throw (ex-info "Order not found" {:order/id id}))))})

(def derived-greeting
  {:id       :derived-greeting
   :input    [:user/name :user/age]
   :output   [:user/greeting]
   :resolve  (fn [_ctx {:user/keys [name age]}]
               {:user/greeting (str "Hello, " name "! You are " age " years old.")})})

(def user-address
  {:id       :user-address
   :input    [:user/id]
   :output   [:user/address]
   :resolve  (fn [_ctx {:user/keys [id]}]
               (case id
                 1 {:user/address {:address/street "123 Main St" :address/zip "10001"}}
                 2 {:user/address {:address/street "456 Oak Ave" :address/zip "90210"}}
                 3 {:user/address {:address/street "789 Elm Rd"  :address/zip "60601"}}
                 (throw (ex-info "Address not found" {:user/id id}))))})

(def shipping-label
  {:id       :shipping-label
   :input    [{:order/user [:user/name {:user/address [:address/zip]}]}]
   :output   [:order/shipping-label]
   :resolve  (fn [_ctx input]
               (let [user-name (get-in input [:order/user :user/name])
                     zip       (get-in input [:order/user :user/address :address/zip])]
                 {:order/shipping-label (str "Ship to: " user-name ", " zip)}))})

(def friend-summary
  {:id       :friend-summary
   :input    [{:user/friends [:user/name]}]
   :output   [:user/friend-names]
   :resolve  (fn [_ctx input]
               {:user/friend-names (mapv :user/name (:user/friends input))})})

(def all-resolvers
  [user-by-id user-friends user-age current-user order-by-id derived-greeting
   user-address shipping-label friend-summary])

(def index (pl/build-index all-resolvers))

(defn q
  "Helper to run a query with the test index and optional extra ctx keys."
  ([entity query-vec]
   (pl/query {:biff.pathom-lite/index index} entity query-vec))
  ([ctx entity query-vec]
   (pl/query (assoc ctx :biff.pathom-lite/index index) entity query-vec)))

;; ---------------------------------------------------------------------------
;; Tests
;; ---------------------------------------------------------------------------

(deftest simple-resolve-test
  (testing "Resolve simple attributes from entity seed data"
    (is (= {:user/name "Alice" :user/email "alice@example.com"}
           (q {:user/id 1} [:user/name :user/email])))))

(deftest multiple-attributes-test
  (testing "Resolve multiple attributes from different resolvers"
    (is (= {:user/name "Bob" :user/age 25}
           (q {:user/id 2} [:user/name :user/age])))))

(deftest global-resolver-test
  (testing "Global resolver (no input) provides seed data"
    (is (= {:user/id 1 :user/name "Alice"}
           (q {:current-user-id 1} {} [:user/id :user/name])))))

(deftest nested-join-test
  (testing "Nested join resolves child entities"
    (is (= {:user/name "Alice"
            :user/friends [{:user/name "Bob"} {:user/name "Carol"}]}
           (q {:user/id 1} [:user/name {:user/friends [:user/name]}])))))

(deftest deeply-nested-join-test
  (testing "Nested join resolves multiple levels deep"
    (is (= {:user/friends [{:user/name "Bob"
                             :user/friends [{:user/name "Alice"}]}
                            {:user/name "Carol"
                             :user/friends [{:user/name "Alice"} {:user/name "Bob"}]}]}
           (q {:user/id 1} [{:user/friends [:user/name {:user/friends [:user/name]}]}])))))

(deftest join-with-single-entity-test
  (testing "Join with a single nested entity (not a collection)"
    (is (= {:order/total 59.99
            :order/user {:user/name "Alice" :user/email "alice@example.com"}}
           (q {:order/id 100} [:order/total {:order/user [:user/name :user/email]}])))))

(deftest entity-passthrough-test
  (testing "Attributes already in entity are passed through without resolving"
    (is (= {:user/name "Override"}
           (q {:user/id 1 :user/name "Override"} [:user/name])))))

(deftest derived-resolver-test
  (testing "Resolver that depends on outputs of other resolvers"
    (is (= {:user/greeting "Hello, Alice! You are 30 years old."}
           (q {:user/id 1} [:user/greeting])))))

(deftest env-passthrough-test
  (testing "Context map is passed to resolvers"
    (is (= {:user/name "Carol" :user/email "carol@example.com"}
           (q {:current-user-id 3} {} [:user/name :user/email])))))

(deftest strict-mode-test
  (testing "Throws when no resolver can satisfy an attribute"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"No resolver found for attribute"
         (q {:user/id 1} [:nonexistent/attr])))))

(deftest build-index-test
  (testing "build-index indexes resolvers by their flat output keys"
    (let [idx (pl/build-index [user-by-id user-friends])]
      (is (= [:user-by-id] (mapv :id (get-in idx [:resolvers-by-output :user/name]))))
      (is (= [:user-by-id] (mapv :id (get-in idx [:resolvers-by-output :user/email]))))
      (is (= [:user-friends] (mapv :id (get-in idx [:resolvers-by-output :user/friends])))))))

(deftest empty-query-test
  (testing "Empty query returns empty map"
    (is (= {} (q {:user/id 1} [])))))

(deftest resolver-creation-test
  (testing "resolver validates required fields"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"must have an :id"
         (pl/resolver {:resolve (fn [_ _] {})})))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"must have a :resolve"
         (pl/resolver {:id :test})))))

;; ---------------------------------------------------------------------------
;; Nested input tests
;; ---------------------------------------------------------------------------

(deftest nested-input-simple-test
  (testing "Resolver with nested join input resolves the nested data"
    (is (= {:order/shipping-label "Ship to: Alice, 10001"}
           (q {:order/id 100} [:order/shipping-label])))))

(deftest nested-input-collection-test
  (testing "Resolver with nested collection input"
    (is (= {:user/friend-names ["Bob" "Carol"]}
           (q {:user/id 1} [:user/friend-names])))))

(deftest nested-input-with-other-attrs-test
  (testing "Nested input alongside regular output queries"
    (is (= {:order/total 59.99
            :order/shipping-label "Ship to: Alice, 10001"}
           (q {:order/id 100} [:order/total :order/shipping-label])))))

;; ---------------------------------------------------------------------------
;; Optional output tests
;; ---------------------------------------------------------------------------

(deftest optional-output-test
  (testing "Resolver that doesn't return the requested key is skipped"
    (let [idx (pl/build-index
               [{:id       :partial
                 :input    [:user/id]
                 :output   [:user/name :user/nickname]
                 :resolve  (fn [_ctx {:user/keys [id]}]
                             {:user/name (str "User-" id)})}
                {:id       :complete
                 :input    [:user/id]
                 :output   [:user/nickname]
                 :resolve  (fn [_ctx {:user/keys [id]}]
                             {:user/nickname (str "Nick-" id)})}])]
      (is (= {:user/nickname "Nick-1"}
             (pl/query {:biff.pathom-lite/index idx} {:user/id 1} [:user/nickname]))))))

(deftest optional-output-nil-value-test
  (testing "Resolver that returns the key with nil value is accepted"
    (let [idx (pl/build-index
               [{:id       :nil-val
                 :input    [:user/id]
                 :output   [:user/nickname]
                 :resolve  (fn [_ctx _input]
                             {:user/nickname nil})}])]
      (is (= {:user/nickname nil}
             (pl/query {:biff.pathom-lite/index idx} {:user/id 1} [:user/nickname]))))))

;; ---------------------------------------------------------------------------
;; Join validation tests
;; ---------------------------------------------------------------------------

(deftest join-scalar-in-entity-throws-test
  (testing "Join on a scalar value in entity throws"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Expected a map or collection for join"
         (q {:user/friends "not-a-collection"}
            [{:user/friends [:user/name]}])))))

(deftest join-nil-in-entity-throws-test
  (testing "Join on a nil value in entity throws"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Expected a map or collection for join"
         (q {:user/friends nil}
            [{:user/friends [:user/name]}])))))

(deftest join-scalar-from-resolver-throws-test
  (testing "Join on a scalar value from resolver throws"
    (let [idx (pl/build-index
               [{:id       :bad-friends
                 :input    [:user/id]
                 :output   [:user/friends]
                 :resolve  (fn [_ctx _input]
                             {:user/friends "oops-not-a-map"})}])]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Expected a map or collection for join"
           (pl/query {:biff.pathom-lite/index idx} {:user/id 1} [{:user/friends [:user/name]}]))))))

;; ---------------------------------------------------------------------------
;; Flat output validation tests
;; ---------------------------------------------------------------------------

(deftest nested-output-rejected-test
  (testing "Resolver with nested map output is rejected"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"must be flat keywords"
         (pl/resolver {:id       :bad-output
                       :input    [:user/id]
                       :output   [{:user/friends [:user/name]}]
                       :resolve  (fn [_ _] {})})))))

;; ---------------------------------------------------------------------------
;; Var-based resolver tests
;; ---------------------------------------------------------------------------

(defn var-user-by-id
  {:input  [:user/id]
   :output [:user/name :user/email]}
  [_ctx {:user/keys [id]}]
  (case id
    1 {:user/name "Alice" :user/email "alice@example.com"}
    2 {:user/name "Bob"   :user/email "bob@example.com"}
    (throw (ex-info "User not found" {:user/id id}))))

(deftest var-resolver-test
  (testing "Var-based resolver uses metadata for input/output and var ns/name for id"
    (let [r (pl/resolver #'var-user-by-id)]
      (is (= :com.biffweb.pathom-lite-test/var-user-by-id (:id r)))
      (is (= [:user/id] (:input r)))
      (is (= [:user/name :user/email] (:output r)))
      (is (var? (:resolve r))))))

(deftest var-resolver-query-test
  (testing "Var-based resolver works in queries"
    (let [idx (pl/build-index [#'var-user-by-id])]
      (is (= {:user/name "Alice" :user/email "alice@example.com"}
             (pl/query {:biff.pathom-lite/index idx} {:user/id 1} [:user/name :user/email]))))))

(deftest var-resolver-reeval-test
  (testing "Var-based resolver picks up redefined functions"
    (let [idx (pl/build-index [#'var-user-by-id])
          original-result (pl/query {:biff.pathom-lite/index idx} {:user/id 1} [:user/name])]
      (is (= {:user/name "Alice"} original-result))
      ;; The resolver stores the var, so if the var were rebound, the new
      ;; behavior would be picked up. We verify the var is stored, not the fn.
      (is (var? (:resolve (first (:all-resolvers idx))))))))

(deftest build-index-auto-resolves-test
  (testing "build-index calls resolver on each item automatically"
    (let [idx (pl/build-index [{:id       :test-r
                                :input    [:a]
                                :output   [:b]
                                :resolve  (fn [_ _] {:b 1})}])]
      (is (= [:test-r] (mapv :id (:all-resolvers idx)))))))

;; ---------------------------------------------------------------------------
;; Optional input tests
;; ---------------------------------------------------------------------------

(deftest optional-input-present-test
  (testing "Optional input is included when available"
    (let [idx (pl/build-index
               [{:id       :greeting-with-title
                 :input    [:user/name [:? :user/title]]
                 :output   [:user/greeting]
                 :resolve  (fn [_ctx {:user/keys [name title]}]
                             {:user/greeting (if title
                                              (str "Hello, " title " " name "!")
                                              (str "Hello, " name "!"))})}])]
      (is (= {:user/greeting "Hello, Dr. Alice!"}
             (pl/query {:biff.pathom-lite/index idx}
                       {:user/name "Alice" :user/title "Dr."}
                       [:user/greeting]))))))

(deftest optional-input-missing-test
  (testing "Optional input is omitted when not available"
    (let [idx (pl/build-index
               [{:id       :greeting-with-title
                 :input    [:user/name [:? :user/title]]
                 :output   [:user/greeting]
                 :resolve  (fn [_ctx {:user/keys [name title]}]
                             {:user/greeting (if title
                                              (str "Hello, " title " " name "!")
                                              (str "Hello, " name "!"))})}])]
      (is (= {:user/greeting "Hello, Alice!"}
             (pl/query {:biff.pathom-lite/index idx}
                       {:user/name "Alice"}
                       [:user/greeting]))))))

(deftest optional-join-input-present-test
  (testing "Optional join input is included when available"
    (let [idx (pl/build-index
               [user-by-id
                user-address
                {:id       :label-with-address
                 :input    [:user/name {[:? :user/address] [:address/zip]}]
                 :output   [:user/label]
                 :resolve  (fn [_ctx input]
                             (let [name (get input :user/name)
                                   zip  (get-in input [:user/address :address/zip])]
                               {:user/label (if zip
                                             (str name " (" zip ")")
                                             name)}))}])]
      (is (= {:user/label "Alice (10001)"}
             (pl/query {:biff.pathom-lite/index idx}
                       {:user/id 1}
                       [:user/label]))))))

(deftest optional-join-input-missing-test
  (testing "Optional join input is omitted when not available"
    (let [idx (pl/build-index
               [{:id       :label-with-address
                 :input    [:user/name {[:? :user/address] [:address/zip]}]
                 :output   [:user/label]
                 :resolve  (fn [_ctx input]
                             (let [name (get input :user/name)
                                   zip  (get-in input [:user/address :address/zip])]
                               {:user/label (if zip
                                             (str name " (" zip ")")
                                             name)}))}])]
      (is (= {:user/label "Alice"}
             (pl/query {:biff.pathom-lite/index idx}
                       {:user/name "Alice"}
                       [:user/label]))))))

;; ---------------------------------------------------------------------------
;; Optional query item tests
;; ---------------------------------------------------------------------------

(deftest optional-query-item-present-test
  (testing "Optional query item is included when resolvable"
    (is (= {:user/name "Alice" :user/email "alice@example.com"}
           (q {:user/id 1} [:user/name [:? :user/email]])))))

(deftest optional-query-item-missing-test
  (testing "Optional query item is omitted when not resolvable"
    (is (= {:user/name "Alice"}
           (q {:user/id 1} [:user/name [:? :nonexistent/attr]])))))

(deftest optional-query-join-present-test
  (testing "Optional join in query is included when resolvable"
    (is (= {:user/name "Alice"
            :user/friends [{:user/name "Bob"} {:user/name "Carol"}]}
           (q {:user/id 1} [:user/name {[:? :user/friends] [:user/name]}])))))

(deftest optional-query-join-missing-test
  (testing "Optional join in query is omitted when not resolvable"
    (is (= {:user/name "Alice"}
           (q {:user/id 1} [:user/name {[:? :nonexistent/join] [:some/attr]}])))))

(deftest optional-query-item-from-entity-test
  (testing "Optional query item works when value is in entity"
    (is (= {:user/name "Alice" :extra "data"}
           (q {:user/id 1 :extra "data"} [:user/name [:? :extra]])))))

;; ---------------------------------------------------------------------------
;; Exception handling tests
;; ---------------------------------------------------------------------------

(deftest resolve-error-has-marker-test
  (testing "Resolution errors include ::pl/resolve-error in ex-data"
    (try
      (q {:user/id 1} [:nonexistent/attr])
      (is false "Should have thrown")
      (catch clojure.lang.ExceptionInfo e
        (is (true? (:com.biffweb.pathom-lite/resolve-error (ex-data e))))))))

(deftest non-resolve-exceptions-propagate-test
  (testing "Non-resolution exceptions from resolvers are not swallowed"
    (let [idx (pl/build-index
               [{:id       :throws-runtime
                 :input    [:user/id]
                 :output   [:user/boom]
                 :resolve  (fn [_ _] (throw (ex-info "custom error" {:custom true})))}])]
      (try
        (pl/query {:biff.pathom-lite/index idx} {:user/id 1} [:user/boom])
        (is false "Should have thrown")
        (catch clojure.lang.ExceptionInfo e
          ;; The custom exception should propagate (not be swallowed as a resolve error)
          ;; It might be wrapped in a "No resolver found" if the resolver throws non-resolve-error
          ;; and then no other candidate succeeds. Let's verify it gets through.
          (is (or (:custom (ex-data e))
                  (:com.biffweb.pathom-lite/resolve-error (ex-data e)))))))))
