(ns com.biffweb.pathom-lite-demo
  "A small web demo for com.biffweb.pathom-lite. Run with: clojure -M -m com.biffweb.pathom-lite-demo"
  (:require [com.biffweb.pathom-lite :as biff.pl]
            [clojure.string :as str]
            [clojure.edn :as edn])
  (:import [com.sun.net.httpserver HttpServer HttpHandler]
           [java.net InetSocketAddress]
           [java.io OutputStream]))

;; ---------------------------------------------------------------------------
;; Sample resolvers for the demo
;; ---------------------------------------------------------------------------

(def users-db
  {1 {:user/name "Alice" :user/email "alice@example.com" :user/age 30}
   2 {:user/name "Bob"   :user/email "bob@example.com"   :user/age 25}
   3 {:user/name "Carol" :user/email "carol@example.com"  :user/age 35}})

(def friends-db
  {1 [2 3]
   2 [1]
   3 [1 2]})

(defn user-by-id
  {:input  [:user/id]
   :output [:user/name :user/email :user/age]}
  [_ctx {:user/keys [id]}]
  (or (get users-db id)
      (throw (ex-info "User not found" {:user/id id}))))

(defn user-friends
  {:input  [:user/id]
   :output [:user/friends]}
  [_ctx {:user/keys [id]}]
  {:user/friends (mapv (fn [fid] {:user/id fid})
                       (get friends-db id []))})

(defn user-greeting
  {:input  [:user/name :user/age]
   :output [:user/greeting]}
  [_ctx {:user/keys [name age]}]
  {:user/greeting (str "Hello, " name "! You are " age " years old.")})

(defn order-by-id
  {:input  [:order/id]
   :output [:order/total :order/status :order/user]}
  [_ctx {:order/keys [id]}]
  (case id
    100 {:order/total 59.99 :order/status :shipped :order/user {:user/id 1}}
    101 {:order/total 12.50 :order/status :pending :order/user {:user/id 2}}
    (throw (ex-info "Order not found" {:order/id id}))))

(defn user-address
  {:input  [:user/id]
   :output [:user/address]}
  [_ctx {:user/keys [id]}]
  (case id
    1 {:user/address {:address/street "123 Main St" :address/zip "10001"}}
    2 {:user/address {:address/street "456 Oak Ave" :address/zip "90210"}}
    3 {:user/address {:address/street "789 Elm Rd"  :address/zip "60601"}}
    (throw (ex-info "Address not found" {:user/id id}))))

(defn shipping-label
  {:input  [{:order/user [:user/name {:user/address [:address/zip]}]}]
   :output [:order/shipping-label]}
  [_ctx input]
  (let [user-name (get-in input [:order/user :user/name])
        zip       (get-in input [:order/user :user/address :address/zip])]
    {:order/shipping-label (str "Ship to: " user-name ", " zip)}))

(def demo-index
  (biff.pl/build-index [#'user-by-id #'user-friends #'user-greeting
                         #'order-by-id #'user-address #'shipping-label]))

;; ---------------------------------------------------------------------------
;; HTML
;; ---------------------------------------------------------------------------

(def example-queries
  [{:label "Simple attributes"
    :entity "{:user/id 1}"
    :query "[:user/name :user/email]"}
   {:label "Nested join"
    :entity "{:user/id 1}"
    :query "[:user/name {:user/friends [:user/name]}]"}
   {:label "Deeply nested"
    :entity "{:user/id 1}"
    :query "[{:user/friends [:user/name {:user/friends [:user/name]}]}]"}
   {:label "Derived resolver (chaining)"
    :entity "{:user/id 2}"
    :query "[:user/greeting]"}
   {:label "Nested input (shipping label)"
    :entity "{:order/id 100}"
    :query "[:order/shipping-label]"}
   {:label "Optional query item"
    :entity "{:user/id 1}"
    :query "[:user/name [:? :nonexistent/attr]]"}
   {:label "Multiple attributes"
    :entity "{:user/id 3}"
    :query "[:user/name :user/age :user/email]"}])

(defn html-page [result-html entity-val query-val]
  (str
   "<!DOCTYPE html><html><head><meta charset='utf-8'><title>pathom-lite demo</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; background: #f8f9fa; color: #333; }
  h1 { color: #1a1a2e; }
  .card { background: white; border-radius: 8px; padding: 20px; margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
  textarea { width: 100%; font-family: 'JetBrains Mono', monospace; font-size: 14px; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }
  button { background: #4361ee; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; font-size: 14px; }
  button:hover { background: #3a56d4; }
  pre { background: #2b2d42; color: #edf2f4; padding: 15px; border-radius: 4px; overflow-x: auto; }
  .examples { display: flex; flex-wrap: wrap; gap: 8px; margin: 10px 0; }
  .examples button { background: #e9ecef; color: #333; font-size: 12px; padding: 6px 12px; }
  .examples button:hover { background: #dee2e6; }
  label { font-weight: bold; display: block; margin: 10px 0 5px; }
</style></head><body>
<h1>\uD83D\uDD25 pathom-lite demo</h1>
<p>A lightweight alternative to <a href='https://pathom3.wsscode.com/'>pathom3</a>. Try running some EQL queries!</p>
<div class='card'>
  <h3>Try an example:</h3>
  <div class='examples'>"
   (apply str (map (fn [{:keys [label entity query]}]
                     (str "<button onclick=\"document.getElementById('entity').value='" entity
                          "';document.getElementById('query').value='" query "'\">"
                          label "</button>"))
                   example-queries))
   "</div>
  <form method='POST' action='/query'>
    <label>Entity (seed data):</label>
    <textarea id='entity' name='entity' rows='2'>" (or entity-val "{:user/id 1}") "</textarea>
    <label>EQL Query:</label>
    <textarea id='query' name='query' rows='3'>" (or query-val "[:user/name :user/email]") "</textarea>
    <br><br><button type='submit'>Run Query</button>
  </form>
</div>"
   (when result-html
     (str "<div class='card'><h3>Result:</h3>" result-html "</div>"))
   "<div class='card'>
  <h3>Available Resolvers</h3>
  <ul>
    <li><b>user-by-id</b> — <code>[:user/id]</code> → <code>[:user/name :user/email :user/age]</code></li>
    <li><b>user-friends</b> — <code>[:user/id]</code> → <code>[:user/friends]</code></li>
    <li><b>user-greeting</b> — <code>[:user/name :user/age]</code> → <code>[:user/greeting]</code> (derived, chains through user-by-id)</li>
    <li><b>user-address</b> — <code>[:user/id]</code> → <code>[:user/address]</code></li>
    <li><b>shipping-label</b> — <code>[{:order/user [:user/name {:user/address [:address/zip]}]}]</code> → <code>[:order/shipping-label]</code> (nested input!)</li>
  </ul>
  <h3>Orders in DB</h3>
  <ul>
    <li>ID 100: $59.99, shipped, user: Alice</li>
    <li>ID 101: $12.50, pending, user: Bob</li>
  </ul>
  <h3>Users in DB</h3>
  <ul>
    <li>ID 1: Alice (age 30, friends: Bob, Carol)</li>
    <li>ID 2: Bob (age 25, friends: Alice)</li>
    <li>ID 3: Carol (age 35, friends: Alice, Bob)</li>
  </ul>
</div>
</body></html>"))

;; ---------------------------------------------------------------------------
;; HTTP server
;; ---------------------------------------------------------------------------

(defn parse-form-body [body-str]
  (into {}
        (map (fn [pair]
               (let [[k v] (str/split pair #"=" 2)]
                 [(java.net.URLDecoder/decode (or k "") "UTF-8")
                  (java.net.URLDecoder/decode (or v "") "UTF-8")])))
        (str/split body-str #"&")))

(defn handle-query [entity-str query-str]
  (try
    (let [entity (edn/read-string entity-str)
          query  (edn/read-string query-str)
          result (biff.pl/query {:biff.pathom-lite/index demo-index}
                                entity
                                query)]
      (str "<pre>" (pr-str result) "</pre>"))
    (catch Exception e
      (str "<pre style='color:#ef476f;background:#2b2d42'>"
           "Error: " (.getMessage e)
           (when-let [d (ex-data e)]
             (str "\nData: " (pr-str d)))
           "</pre>"))))

(defn handler [exchange]
  (let [method (.getRequestMethod exchange)
        path   (.getPath (.getRequestURI exchange))]
    (cond
      (and (= method "GET") (or (= path "/") (= path "")))
      (let [body (.getBytes (html-page nil nil nil) "UTF-8")]
        (.getResponseHeaders exchange)
        (.sendResponseHeaders exchange 200 (count body))
        (with-open [os (.getResponseBody exchange)]
          (.write os body)))

      (and (= method "POST") (= path "/query"))
      (let [is   (.getRequestBody exchange)
            buf  (.readAllBytes is)
            form (parse-form-body (String. buf "UTF-8"))
            entity-str (get form "entity" "{}")
            query-str  (get form "query" "[]")
            result-html (handle-query entity-str query-str)
            body (.getBytes (html-page result-html entity-str query-str) "UTF-8")]
        (.sendResponseHeaders exchange 200 (count body))
        (with-open [os (.getResponseBody exchange)]
          (.write os body)))

      :else
      (let [body (.getBytes "Not Found" "UTF-8")]
        (.sendResponseHeaders exchange 404 (count body))
        (with-open [os (.getResponseBody exchange)]
          (.write os body))))))

(defn -main [& _args]
  (let [port   (Integer/parseInt (or (System/getenv "PORT") "8080"))
        server (HttpServer/create (InetSocketAddress. port) 0)]
    (.createContext server "/" (reify HttpHandler
                                (handle [_ exchange]
                                  (try
                                    (handler exchange)
                                    (catch Exception e
                                      (.printStackTrace e)
                                      (let [body (.getBytes (str "Internal error: " (.getMessage e)) "UTF-8")]
                                        (.sendResponseHeaders exchange 500 (count body))
                                        (with-open [os (.getResponseBody exchange)]
                                          (.write os body))))))))
    (.setExecutor server nil)
    (.start server)
    (println (str "pathom-lite demo running on http://localhost:" port))))
