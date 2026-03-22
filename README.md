# biff-pathom-lite

A lightweight alternative to [pathom3](https://pathom3.wsscode.com/) for Clojure.

This library provides a minimal resolver-based data fetching engine that supports:

- **Simple resolvers** with declared inputs and outputs
- **Nested queries** (EQL-style joins)
- **Global resolvers** (no input required)
- **Transitive resolution** — automatically chains resolvers to satisfy dependencies
- **Strict mode** — throws when data can't be resolved

## What's omitted (vs pathom3)

- Plugin system
- Lenient mode
- Batch resolvers
- Query planning (uses the input query directly)
- EQL AST manipulation
- Optional resolvers

## Usage

Add to your `deps.edn`:

```clojure
{:deps {com.biffweb/pathom-lite {:git/url "https://github.com/jacobobryant/biff-pathom-lite"
                                  :git/sha "..."}}}
```

### Define resolvers

```clojure
(require '[com.biffweb.pathom-lite :as pl])

(def user-by-id
  (pl/resolver
   {:name    :user-by-id
    :input   [:user/id]
    :output  [:user/name :user/email]
    :resolve (fn [env {:user/keys [id]}]
               ;; fetch from db, etc.
               {:user/name "Alice" :user/email "alice@example.com"})}))

(def user-friends
  (pl/resolver
   {:name    :user-friends
    :input   [:user/id]
    :output  [{:user/friends [:user/id :user/name]}]
    :resolve (fn [env {:user/keys [id]}]
               {:user/friends [{:user/id 2} {:user/id 3}]})}))
```

### Run a query

```clojure
(pl/process
 {:resolvers [user-by-id user-friends]
  :entity    {:user/id 1}
  :query     [:user/name {:user/friends [:user/name]}]})
;; => {:user/name "Alice"
;;     :user/friends [{:user/name "Bob"} {:user/name "Carol"}]}
```

### Options

| Key          | Description                                          |
|-------------|------------------------------------------------------|
| `:resolvers` | Collection of resolver maps                         |
| `:query`     | EQL query vector                                     |
| `:entity`    | (optional) Initial entity map with seed data         |
| `:env`       | (optional) Environment map passed to resolver fns    |

## Running tests

```bash
clojure -X:test
```

## License

MIT
