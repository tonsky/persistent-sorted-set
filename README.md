A B-tree based persistent sorted set for Clojure/Script.

PersistentSortedSet supports:

- transients,
- custom comparators,
- fast iteration,
- efficient slices (iterator over a part of the set)
- efficient `rseq` on slices.

Almost a drop-in replacement for `clojure.core/sorted-set`, the only difference being this one can’t store `nil`.

Implementations are provided for Clojure and ClojureScript.

## Building

```
export JAVA8_HOME="/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home"
lein jar
```

## Support us

<a href="https://www.patreon.com/bePatron?u=4230547"><img src="./extras/become_a_patron_button@2x.png" alt="Become a Patron!" width="217" height="51"></a>

## Usage

Dependency:

```clj
[persistent-sorted-set "0.2.3"]
```

Code:

```clj
(require '[me.tonsky.persistent-sorted-set :as set])

(set/sorted-set 3 2 1)
;=> #{1 2 3}

(-> (set/sorted-set 1 2 3 4)
    (conj 2.5))
;=> #{1 2 2.5 3 4}

(-> (set/sorted-set 1 2 3 4)
    (disj 3))
;=> #{1 2 4}

(-> (set/sorted-set 1 2 3 4)
    (contains? 3))
;=> true

(-> (apply set/sorted-set (range 10000))
    (set/slice 5000 5010))
;=> (5000 5001 5002 5003 5004 5005 5006 5007 5008 5009 5010)

(-> (apply set/sorted-set (range 10000))
    (set/rslice 5010 5000))
;=> (5010 5009 5008 5007 5006 5005 5004 5003 5002 5001 5000)

(set/sorted-set-by > 1 2 3)
;=> #{3 2 1}
```
One can also efficiently seek on the iterators.

```clj
(-> (seq (into (set/sorted-set) (range 10)))
    (set/seek 5))
;; => (5 6 7 8 9)

(-> (into (set/sorted-set) (range 100))
    (set/rslice 75 25)
    (set/seek 60)
    (set/seek 30))
;; => (30 29 28 27 26 25)
```

## Durability

Clojure version allows efficient storage of Persistent Sorted Set on disk/DB/anywhere.

To do that, implement `IStorage` interface:

```clojure
(defrecord Storage [*storage]
  IStorage
  (store [_ node]
    (let [address (random-uuid)]
      (swap! *storage assoc address
        (pr-str
          (if (instance? Branch node)
            {:level     (.level ^Branch node)
             :keys      (.keys ^Branch node)
             :addresses (.addresses ^Branch node)}
            (.keys ^Leaf node))))
      address))
    
  (restore [_ address]
    (let [value (-> (get @*storage address)
                  (edn/read-string))]
      (if (map? value)
        (Branch. (int (:level value)) ^java.util.List (:keys value) ^java.util.List (:addresses value))
        (Leaf. ^java.util.List value)))))
```

Storing Persistent Sorted Set works per node. This will save each node once:

```clojure
(def set
  (into (set/sorted-set) (range 1000000)))

(def storage
  (Storage. (atom {})))

(def root
  (set/store set storage))
```

If you try to store once again, no store operations will be issued:

```clojure
(assert
  (= root
    (set/store set storage)))
```

If you modify set and store new one, only nodes that were changed will be stored. For a tree of depth 3, it’s usually just \~3 nodes. The root will be new, though:

```clojure
(def set2
  (into set [-1 -2 -3]))

(assert
  (not= root
    (set/store set2 storage)))
```

Finally, one can construct a new set from its stored snapshot. You’ll need address for that:

```clojure
(def set-lazy
  (set/restore root storage))
```

Restore operation is lazy. By default it won’t do anything, but when you start accessing returned set, `IStorage::restore` operations will be issued and part of the set will be reconstructed in memory. Only nodes needed for a particular operation will be loaded.

E.g. this will load \~3 nodes for a set of depth 3:

```clojure
(first set-lazy)
```

This will load \~50 nodes on default settings:

```clojure
(take 5000 set-lazy)
```

Internally Persistent Sorted Set does not caches returned nodes, so don’t be surprised if subsequent `first` loads the same nodes again. One must implement cache inside IStorage implementation for efficient retrieval of already loaded nodes. Also see `IStorage::accessed` for access stats, e.g. for LRU.

Any operation that can be done on in-memory PSS can be done on a lazy one, too. It will fetch required nodes when needed, completely transparently for the user. Lazy PSS can exist arbitrary long without ever being fully realized in memory:

```clojure
(def set3
  (conj set-lazy [-1 -2 -3]))

(def set4
  (disj set-lazy [4 5 6 7 8]))

(contains? set-lazy 5000)
```

Last piece of the puzzle: `set/walk-addresses`. Use it to check which nodes are actually in use by current PSS and optionally clean up garbage in your storage that is not referenced by it anymore:

```clojure
(let [*alive-addresses (volatile! [])]
  (set/walk-addresses set #(vswap! *alive-addresses conj %))
  @*alive-addresses)
```

See [test_storage.clj](test-clojure/me/tonsky/persistent_sorted_set/test_storage.clj) for more examples.

Durability for ClojureScript is not yet supported.

## Performance

To reproduce:

1. Install `[com.datomic/datomic-free "0.9.5703"]` locally.
2. Run `lein bench`.

`PersistentTreeSet` is Clojure’s Red-black tree based sorted-set.
`BTSet` is Datomic’s B-tree based sorted set (no transients, no disjoins).
`PersistentSortedSet` is this implementation.

Numbers I get on my 3.2 GHz i7-8700B:

### Conj 100k randomly sorted Integers

```
PersistentTreeSet                 143..165ms  
BTSet                             125..141ms  
PersistentSortedSet               105..121ms  
PersistentSortedSet (transient)   50..54ms    
```

### Call contains? 100k times with random Integer on a 100k Integers set

```
PersistentTreeSet     51..54ms    
BTSet                 45..47ms    
PersistentSortedSet   46..47ms    
```

### Iterate with java.util.Iterator over a set of 1M Integers

```
PersistentTreeSet     70..77ms    
PersistentSortedSet   10..11ms    
```

### Iterate with ISeq.first/ISeq.next over a set of 1M Integers

```
PersistentTreeSet     116..124ms  
BTSet                 92..105ms   
PersistentSortedSet   56..68ms    
```

### Iterate over a part of a set from 1 to 999999 in a set of 1M Integers

For `PersistentTreeSet` we use ISeq produced by `(take-while #(<= % 999999) (.seqFrom set 1 true))`.

For `PersistentSortedSet` we use `(.slice set 1 999999)`.

```
PersistentTreeSet     238..256ms  
PersistentSortedSet   70..91ms    
```

### Disj 100k elements in randomized order from a set of 100k Integers

```
PersistentTreeSet                 151..155ms  
PersistentSortedSet               91..98ms    
PersistentSortedSet (transient)   47..50ms    
```

## Projects using PersistentSortedSet

- [Datascript](https://github.com/tonsky/datascript), persistent in-memory database

## License

Copyright © 2019 Nikita Prokopov

Licensed under MIT (see [LICENSE](LICENSE)).
