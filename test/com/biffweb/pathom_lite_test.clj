(ns com.biffweb.pathom-lite-test
  (:require [clojure.test :refer [deftest is testing]]
            [com.biffweb.pathom-lite :as pl]))

;; ---------------------------------------------------------------------------
;; Test data: resolvers
;; ---------------------------------------------------------------------------

(def user-by-id
  (pl/resolver
   {:name    :user-by-id
    :input   [:user/id]
    :output  [:user/name :user/email]
    :resolve (fn [_env {:user/keys [id]}]
               (case id
                 1 {:user/name "Alice" :user/email "alice@example.com"}
                 2 {:user/name "Bob"   :user/email "bob@example.com"}
                 3 {:user/name "Carol" :user/email "carol@example.com"}
                 (throw (ex-info "User not found" {:user/id id}))))}))

(def user-friends
  (pl/resolver
   {:name    :user-friends
    :input   [:user/id]
    :output  [:user/friends]
    :resolve (fn [_env {:user/keys [id]}]
               (case id
                 1 {:user/friends [{:user/id 2} {:user/id 3}]}
                 2 {:user/friends [{:user/id 1}]}
                 3 {:user/friends [{:user/id 1} {:user/id 2}]}
                 {:user/friends []}))}))

(def user-age
  (pl/resolver
   {:name    :user-age
    :input   [:user/id]
    :output  [:user/age]
    :resolve (fn [_env {:user/keys [id]}]
               (case id
                 1 {:user/age 30}
                 2 {:user/age 25}
                 3 {:user/age 35}
                 {:user/age nil}))}))

(def current-user
  (pl/resolver
   {:name    :current-user
    :input   []
    :output  [:user/id]
    :resolve (fn [env _input]
               {:user/id (:current-user-id env)})}))

(def order-by-id
  (pl/resolver
   {:name    :order-by-id
    :input   [:order/id]
    :output  [:order/total :order/status :order/user]
    :resolve (fn [_env {:order/keys [id]}]
               (case id
                 100 {:order/total 59.99 :order/status :shipped :order/user {:user/id 1}}
                 101 {:order/total 12.50 :order/status :pending :order/user {:user/id 2}}
                 (throw (ex-info "Order not found" {:order/id id}))))}))

(def derived-greeting
  (pl/resolver
   {:name    :derived-greeting
    :input   [:user/name :user/age]
    :output  [:user/greeting]
    :resolve (fn [_env {:user/keys [name age]}]
               {:user/greeting (str "Hello, " name "! You are " age " years old.")})}))

(def user-address
  (pl/resolver
   {:name    :user-address
    :input   [:user/id]
    :output  [:user/address]
    :resolve (fn [_env {:user/keys [id]}]
               (case id
                 1 {:user/address {:address/street "123 Main St" :address/zip "10001"}}
                 2 {:user/address {:address/street "456 Oak Ave" :address/zip "90210"}}
                 3 {:user/address {:address/street "789 Elm Rd"  :address/zip "60601"}}
                 (throw (ex-info "Address not found" {:user/id id}))))}))

(def shipping-label
  (pl/resolver
   {:name    :shipping-label
    :input   [{:order/user [:user/name {:user/address [:address/zip]}]}]
    :output  [:order/shipping-label]
    :resolve (fn [_env input]
               (let [user-name (get-in input [:order/user :user/name])
                     zip       (get-in input [:order/user :user/address :address/zip])]
                 {:order/shipping-label (str "Ship to: " user-name ", " zip)}))}))

(def friend-summary
  (pl/resolver
   {:name    :friend-summary
    :input   [{:user/friends [:user/name]}]
    :output  [:user/friend-names]
    :resolve (fn [_env input]
               {:user/friend-names (mapv :user/name (:user/friends input))})}))

(def all-resolvers
  [user-by-id user-friends user-age current-user order-by-id derived-greeting
   user-address shipping-label friend-summary])

;; ---------------------------------------------------------------------------
;; Tests
;; ---------------------------------------------------------------------------

(deftest simple-resolve-test
  (testing "Resolve simple attributes from entity seed data"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:user/id 1}
                   :query     [:user/name :user/email]})]
      (is (= {:user/name "Alice" :user/email "alice@example.com"} result)))))

(deftest multiple-attributes-test
  (testing "Resolve multiple attributes from different resolvers"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:user/id 2}
                   :query     [:user/name :user/age]})]
      (is (= {:user/name "Bob" :user/age 25} result)))))

(deftest global-resolver-test
  (testing "Global resolver (no input) provides seed data"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :env       {:current-user-id 1}
                   :query     [:user/id :user/name]})]
      (is (= {:user/id 1 :user/name "Alice"} result)))))

(deftest nested-join-test
  (testing "Nested join resolves child entities"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:user/id 1}
                   :query     [:user/name {:user/friends [:user/name]}]})]
      (is (= {:user/name "Alice"
              :user/friends [{:user/name "Bob"} {:user/name "Carol"}]}
             result)))))

(deftest deeply-nested-join-test
  (testing "Nested join resolves multiple levels deep"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:user/id 1}
                   :query     [{:user/friends [:user/name {:user/friends [:user/name]}]}]})]
      (is (= {:user/friends [{:user/name "Bob"
                               :user/friends [{:user/name "Alice"}]}
                              {:user/name "Carol"
                               :user/friends [{:user/name "Alice"} {:user/name "Bob"}]}]}
             result)))))

(deftest join-with-single-entity-test
  (testing "Join with a single nested entity (not a collection)"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:order/id 100}
                   :query     [:order/total {:order/user [:user/name :user/email]}]})]
      (is (= {:order/total 59.99
              :order/user {:user/name "Alice" :user/email "alice@example.com"}}
             result)))))

(deftest entity-passthrough-test
  (testing "Attributes already in entity are passed through without resolving"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:user/id 1 :user/name "Override"}
                   :query     [:user/name]})]
      (is (= {:user/name "Override"} result)))))

(deftest derived-resolver-test
  (testing "Resolver that depends on outputs of other resolvers"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:user/id 1}
                   :query     [:user/greeting]})]
      (is (= {:user/greeting "Hello, Alice! You are 30 years old."} result)))))

(deftest env-passthrough-test
  (testing "Environment map is passed to resolvers"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :env       {:current-user-id 3}
                   :query     [:user/name :user/email]})]
      (is (= {:user/name "Carol" :user/email "carol@example.com"} result)))))

(deftest strict-mode-test
  (testing "Throws when no resolver can satisfy an attribute"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"No resolver found for attribute"
         (pl/process
          {:resolvers all-resolvers
           :entity    {:user/id 1}
           :query     [:nonexistent/attr]})))))

(deftest build-index-test
  (testing "build-index indexes resolvers by their flat output keys"
    (let [index (pl/build-index [user-by-id user-friends])]
      (is (= [user-by-id] (get-in index [:resolvers-by-output :user/name])))
      (is (= [user-by-id] (get-in index [:resolvers-by-output :user/email])))
      (is (= [user-friends] (get-in index [:resolvers-by-output :user/friends]))))))

(deftest empty-query-test
  (testing "Empty query returns empty map"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:user/id 1}
                   :query     []})]
      (is (= {} result)))))

(deftest resolver-creation-test
  (testing "resolver validates required fields"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"must have a :name"
         (pl/resolver {:resolve (fn [_ _] {})})))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"must have a :resolve"
         (pl/resolver {:name :test})))))

;; ---------------------------------------------------------------------------
;; Nested input tests
;; ---------------------------------------------------------------------------

(deftest nested-input-simple-test
  (testing "Resolver with nested join input resolves the nested data"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:order/id 100}
                   :query     [:order/shipping-label]})]
      (is (= {:order/shipping-label "Ship to: Alice, 10001"} result)))))

(deftest nested-input-collection-test
  (testing "Resolver with nested collection input"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:user/id 1}
                   :query     [:user/friend-names]})]
      (is (= {:user/friend-names ["Bob" "Carol"]} result)))))

(deftest nested-input-with-other-attrs-test
  (testing "Nested input alongside regular output queries"
    (let [result (pl/process
                  {:resolvers all-resolvers
                   :entity    {:order/id 100}
                   :query     [:order/total :order/shipping-label]})]
      (is (= {:order/total 59.99
              :order/shipping-label "Ship to: Alice, 10001"}
             result)))))

;; ---------------------------------------------------------------------------
;; Optional output tests
;; ---------------------------------------------------------------------------

(deftest optional-output-test
  (testing "Resolver that doesn't return the requested key is skipped"
    (let [partial-resolver (pl/resolver
                            {:name    :partial
                             :input   [:user/id]
                             :output  [:user/name :user/nickname]
                             :resolve (fn [_env {:user/keys [id]}]
                                        ;; Only returns :user/name, not :user/nickname
                                        {:user/name (str "User-" id)})})
          complete-resolver (pl/resolver
                             {:name    :complete
                              :input   [:user/id]
                              :output  [:user/nickname]
                              :resolve (fn [_env {:user/keys [id]}]
                                         {:user/nickname (str "Nick-" id)})})]
      (is (= {:user/nickname "Nick-1"}
             (pl/process {:resolvers [partial-resolver complete-resolver]
                          :entity    {:user/id 1}
                          :query     [:user/nickname]}))))))

(deftest optional-output-nil-value-test
  (testing "Resolver that returns the key with nil value is accepted"
    (let [nil-resolver (pl/resolver
                        {:name    :nil-val
                         :input   [:user/id]
                         :output  [:user/nickname]
                         :resolve (fn [_env _input]
                                    {:user/nickname nil})})]
      (is (= {:user/nickname nil}
             (pl/process {:resolvers [nil-resolver]
                          :entity    {:user/id 1}
                          :query     [:user/nickname]}))))))

;; ---------------------------------------------------------------------------
;; Join validation tests
;; ---------------------------------------------------------------------------

(deftest join-scalar-in-entity-throws-test
  (testing "Join on a scalar value in entity throws"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Expected a map or collection for join"
         (pl/process
          {:resolvers all-resolvers
           :entity    {:user/friends "not-a-collection"}
           :query     [{:user/friends [:user/name]}]})))))

(deftest join-nil-in-entity-throws-test
  (testing "Join on a nil value in entity throws"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Expected a map or collection for join"
         (pl/process
          {:resolvers all-resolvers
           :entity    {:user/friends nil}
           :query     [{:user/friends [:user/name]}]})))))

(deftest join-scalar-from-resolver-throws-test
  (testing "Join on a scalar value from resolver throws"
    (let [bad-resolver (pl/resolver
                        {:name    :bad-friends
                         :input   [:user/id]
                         :output  [:user/friends]
                         :resolve (fn [_env _input]
                                    {:user/friends "oops-not-a-map"})})]
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Expected a map or collection for join"
           (pl/process
            {:resolvers [bad-resolver]
             :entity    {:user/id 1}
             :query     [{:user/friends [:user/name]}]}))))))

;; ---------------------------------------------------------------------------
;; Flat output validation tests
;; ---------------------------------------------------------------------------

(deftest nested-output-rejected-test
  (testing "Resolver with nested map output is rejected"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"must be flat keywords"
         (pl/resolver {:name    :bad-output
                       :input   [:user/id]
                       :output  [{:user/friends [:user/name]}]
                       :resolve (fn [_ _] {})})))))
