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
[persistent-sorted-set "0.1.3"]
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
