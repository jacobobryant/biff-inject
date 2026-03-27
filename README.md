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
- **Batch resolvers** (process multiple entities at once, breadth-first)
- **Transitive resolution** — automatically chains resolvers to satisfy dependencies
- **Strict mode** — throws when data can't be resolved

## What's omitted (vs pathom3)

- Plugin system
- Lenient mode
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

### Batch resolvers

Add `:batch true` to a resolver to make it accept a vector of input maps and return
a vector of output maps (in the same order). Batch resolvers are used automatically
when processing sequential join values (e.g. a list of friends):

```clojure
(defn user-by-id
  {:input [:user/id]
   :output [:user/name :user/email]
   :batch true}
  [ctx inputs]
  ;; inputs is a vector of maps, e.g. [{:user/id 1} {:user/id 2}]
  (mapv (fn [{:user/keys [id]}]
          ;; fetch from db in bulk...
          {:user/name (str "User-" id)
           :user/email (str "user" id "@example.com")})
        inputs))
```

Batch resolvers use **breadth-first traversal**: when a query has nested joins,
all child entities across all parents at a given depth are collected and processed
together. This means a batch resolver at depth N is called exactly once for all
entities at that depth, regardless of how many parents exist.

For example, given this query:

```clojure
[:a {:b [{:c [:d]}]}]
```

If `:b` and `:c` resolve to vectors, the batch resolver for `:d` is called once
with ALL `:c` entities from ALL `:b` parents — not once per `:b` value.

Batch resolvers also work in single-entity contexts (the input is automatically
wrapped in a vector and the result unwrapped).

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

### Batch querying (vector of entities)

`query` accepts either a single entity map or a vector of entity maps. When given
a vector, it returns a vector of result maps — and uses batch resolvers to process
all entities efficiently:

```clojure
(biff.pl/query {:biff.pathom-lite/index index}
               [{:user/id 1} {:user/id 2} {:user/id 3}]
               [:user/name])
;; => [{:user/name "Alice"} {:user/name "Bob"} {:user/name "Carol"}]
```

### API

| Function            | Description                                                     |
|--------------------|-----------------------------------------------------------------|
| `biff.pl/build-index` | Build an index from a collection of resolvers (vars or maps) |
| `biff.pl/query`       | Run an EQL query: `(query ctx entity-or-entities query-vec)`  |
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
