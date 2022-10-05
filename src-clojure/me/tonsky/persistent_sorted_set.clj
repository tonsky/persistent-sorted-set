(ns ^{:author "Nikita Prokopov"
      :doc "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."}
  me.tonsky.persistent-sorted-set
  (:refer-clojure :exclude [conj disj load sorted-set sorted-set-by])
  (:require
    [me.tonsky.persistent-sorted-set.arrays :as arrays])
  (:import
    [java.util Comparator Arrays]
    [java.util.concurrent.atomic AtomicBoolean]
    [me.tonsky.persistent_sorted_set ArrayUtil IStorage Node PersistentSortedSet]))

(set! *warn-on-reflection* true)

(defn conj
  "Analogue to [[clojure.core/conj]] but with comparator that overrides the one stored in set."
  [^PersistentSortedSet set key ^Comparator cmp]
  (.cons set key cmp))


(defn disj
  "Analogue to [[clojure.core/disj]] with comparator that overrides the one stored in set."
  [^PersistentSortedSet set key ^Comparator cmp]
  (.disjoin set key cmp))


(defn slice
  "An iterator for part of the set with provided boundaries.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   `(slice set from nil)` returns iterator for all Xs where X >= from.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^PersistentSortedSet set from to]
    (.slice set from to))
  ([^PersistentSortedSet set from to ^Comparator cmp]
    (.slice set from to cmp)))


(defn rslice
  "A reverse iterator for part of the set with provided boundaries.
   `(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   `(rslice set from nil)` returns backwards iterator for all Xs where X <= from.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^PersistentSortedSet set from to]
    (.rslice set from to))
  ([^PersistentSortedSet set from to ^Comparator cmp]
    (.rslice set from to cmp)))


(defn- array-from-indexed [coll type from to]
  (cond
    (instance? clojure.lang.Indexed coll)
    (ArrayUtil/indexedToArray type coll from to)

    (arrays/array? coll)
    (Arrays/copyOfRange coll from to (arrays/array-type type))))


(defn- split
  ([coll to type avg max]
   (persistent! (split (transient []) 0 coll to type avg max)))
  ([res from coll to type avg max]
   (let [len (- to from)]
     (cond
       (== 0 len)
       res

       (>= len (* 2 avg))
       (recur (conj! res (array-from-indexed coll type from (+ from avg))) (+ from avg) coll to type avg max)

       (<= len max)
       (conj! res (array-from-indexed coll type from to))

       :else
       (-> res
         (conj! (array-from-indexed coll type from (+ from (quot len 2))))
         (conj! (array-from-indexed coll type (+ from (quot len 2)) to)))))))


(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([^Comparator cmp keys]
   (from-sorted-array cmp keys (arrays/alength keys)))
  ([^Comparator cmp keys len]
   (let [max     PersistentSortedSet/MAX_LEN
         avg     (quot (+ PersistentSortedSet/MIN_LEN max) 2)
         storage nil
         edit    nil
         ->Leaf  (fn [keys]
                   (Node. (count keys) keys edit))
         ->Node  (fn [^"[Lme.tonsky.persistent_sorted_set.Node;" children]
                   (Node.
                     (count children)
                     ^objects (arrays/amap #(.maxKey ^Node %) Object children)
                     (object-array (count children))
                     children
                     edit))]
     (loop [nodes (mapv ->Leaf (split keys len Object avg max))]
       (case (count nodes)
         0 (PersistentSortedSet. cmp)
         1 (PersistentSortedSet. {} cmp nil storage (first nodes) len edit 0)
         (recur (mapv ->Node (split nodes (count nodes) Node avg max))))))))


(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  [^Comparator cmp keys]
  (let [arr (to-array keys)
        _   (arrays/asort arr cmp)
        len (ArrayUtil/distinct cmp arr)]
    (from-sorted-array cmp arr len)))


(defn sorted-set-by
  "Create a set with custom comparator."
  ([cmp] (PersistentSortedSet. ^Comparator cmp))
  ([cmp & keys] (from-sequential cmp keys)))


(defn sorted-set
  "Create a set with default comparator."
  ([] (PersistentSortedSet/EMPTY))
  ([& keys] (from-sequential compare keys)))


(defn load [^Comparator cmp ^IStorage storage address]
  (PersistentSortedSet. nil cmp address storage nil -1 nil 0))


(defn set-branching-factor!
  "Global -- applies to all sets. Must be power of 2. Defaults to 64"
  [n]
  (PersistentSortedSet/setMaxLen n))


(defn stats [^PersistentSortedSet set]
  (let [address       (.-_address set)
        root          (.-_root set)
        storage       (.-_storage set)
        loaded-ratio  (fn loaded-ratio [^Node node]
                        (cond 
                          (nil? node)  0.0
                          (.leaf node) 1.0
                          :else
                          (let [len (.len node)
                                children (take len)]
                            (/ (->> (.-_children node) (take len) (map loaded-ratio) (reduce + 0))
                              len))))
        durable-ratio (fn durable-ratio [address ^Node node]
                        (cond 
                          (some? address) 1.0
                          (.leaf node)    0.0
                          :else
                          (let [len (.len node)
                                children (take len)]
                            (/ (->>
                                 (map
                                   (fn [_ addr child]
                                     (durable-ratio addr child))
                                   (range len)
                                   (.-_addresses node)
                                   (.-_children node))
                                 (reduce + 0))
                              len))))]
    {:loaded-ratio  (if (some? root)
                      (double (loaded-ratio root))
                      0.0)
     :durable-ratio (double (durable-ratio address root))}))
