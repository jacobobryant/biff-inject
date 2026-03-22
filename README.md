# biff-pathom-lite

A lightweight alternative to [pathom3](https://pathom3.wsscode.com/) for Clojure.

This library provides a minimal resolver-based data fetching engine that supports:

- **Simple resolvers** with declared inputs and outputs
- **Nested queries** (EQL-style joins)
- **Nested inputs** (resolvers that require sub-attributes of their inputs)
- **Optional inputs** (`[:? :key]` syntax)
- **Optional query items** (`[:? :key]` in query vectors)
- **Global resolvers** (no input required)
- **Var-based resolvers** (metadata-driven, REPL-friendly)
- **Transitive resolution** — automatically chains resolvers to satisfy dependencies
- **Strict mode** — throws when data can't be resolved

## What's omitted (vs pathom3)

- Plugin system
- Lenient mode
- Batch resolvers
- Query planning (uses the input query directly)
- EQL AST manipulation

## Usage

Add to your `deps.edn`:

```clojure
{:deps {com.biffweb/pathom-lite {:git/url "https://github.com/jacobobryant/biff-pathom-lite"
                                  :git/sha "..."}}}
```

### Define resolvers

Define resolvers as regular functions with metadata:

```clojure
(require '[com.biffweb.pathom-lite :as biff.pl])

(defn user-by-id
  {:input [:user/id]
   :output [:user/name :user/email]}
  [ctx {:user/keys [id]}]
  ;; fetch from db, etc.
  {:user/name "Alice" :user/email "alice@example.com"})

(defn user-friends
  {:input [:user/id]
   :output [:user/friends]}
  [ctx {:user/keys [id]}]
  {:user/friends [{:user/id 2} {:user/id 3}]})
```

### Build an index

```clojure
(def index (biff.pl/build-index [#'user-by-id #'user-friends]))
```

### Run a query

```clojure
(biff.pl/query {:biff.pathom-lite/index index}
               {:user/id 1}
               [:user/name {:user/friends [:user/name]}])
;; => {:user/name "Alice"
;;     :user/friends [{:user/name "Bob"} {:user/name "Carol"}]}
```

### Optional inputs

Use `[:? :key]` to mark inputs as optional. When the optional input can't be resolved,
it is simply omitted from the input map passed to the resolver:

```clojure
(defn user-greeting
  {:input [:user/name [:? :user/title]]
   :output [:user/greeting]}
  [ctx {:user/keys [name title]}]
  {:user/greeting (if title
                    (str "Hello, " title " " name "!")
                    (str "Hello, " name "!"))})
```

Optional join inputs are also supported:

```clojure
:input [:user/name {[:? :user/address] [:address/zip]}]
```

### Optional query items

You can also mark query items as optional. When a query item can't be resolved,
it is simply omitted from the result instead of throwing:

```clojure
(biff.pl/query {:biff.pathom-lite/index index}
               {:user/id 1}
               [:user/name [:? :user/nickname]])
;; => {:user/name "Alice"}  ; :user/nickname omitted if no resolver
```

Optional joins in queries:

```clojure
[:user/name {[:? :user/extra] [:extra/info]}]
```

### Map-based resolvers

You can also define resolvers as plain maps:

```clojure
(def my-resolver
  {:id      :my-resolver
   :input   [:some/input]
   :output  [:some/output]
   :resolve (fn [ctx input] {:some/output "value"})})

(def index (biff.pl/build-index [my-resolver]))
```

### API

| Function            | Description                                                     |
|--------------------|-----------------------------------------------------------------|
| `biff.pl/build-index` | Build an index from a collection of resolvers (vars or maps) |
| `biff.pl/query`       | Run an EQL query: `(query ctx entity query-vec)`              |
| `biff.pl/resolver`    | Normalize a resolver (var or map) into a resolver map         |

The context map (`ctx`) passed to `query` must include `:biff.pathom-lite/index`
(the result of `build-index`). Any other keys in ctx are passed through to resolver
functions.

## Running tests

```bash
clojure -X:test
```

## License

MIT
