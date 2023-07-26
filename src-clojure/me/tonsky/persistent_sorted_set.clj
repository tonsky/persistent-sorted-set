(ns ^{:author "Nikita Prokopov"
      :doc "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."}
  me.tonsky.persistent-sorted-set
  (:refer-clojure :exclude [conj disj sorted-set sorted-set-by])
  (:require
    [me.tonsky.persistent-sorted-set.arrays :as arrays])
  (:import
    [clojure.lang RT]
    [java.lang.ref SoftReference]
    [java.util Comparator Arrays]
    [java.util.function BiConsumer]
    [me.tonsky.persistent_sorted_set ANode ArrayUtil Branch IStorage Leaf PersistentSortedSet RefType Settings Seq]))

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

(defn seek
  "An efficient way to seek to a specific key in a seq (either returned by [[clojure.core.seq]] or a slice.)
  `(seek (seq set) to)` returns iterator for all Xs where to <= X.
  Optionally pass in comparator that will override the one that set uses."
  ([seq to]
   (.seek ^Seq seq to))
  ([seq to cmp]
   (.seek ^Seq seq to ^Comparator cmp)))

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

(defn- map->settings ^Settings [m]
  (Settings.
    (int (or (:max-len m) 0))
    (case (:ref-type m)
      :strong RefType/STRONG
      :soft   RefType/SOFT
      :weak   RefType/WEAK
      nil)))

(defn- settings->map [^Settings s]
  {:max-len  (.maxLen s)
   :ref-type (condp identical? (.refType s)
               RefType/STRONG :strong
               RefType/SOFT   :soft
               RefType/WEAK   :weak)})

(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([^Comparator cmp keys]
   (from-sorted-array cmp keys (arrays/alength keys) (Settings.)))
  ([^Comparator cmp keys len]
   (from-sorted-array cmp keys len (Settings.)))
  ([^Comparator cmp keys len opts]
   (let [settings  (map->settings opts)
         max       (.maxLen settings)
         avg       (-> (.minLen settings) (+ max) (quot 2))
         storage   nil
         ->Leaf    (fn [keys]
                     (Leaf. (count keys) keys settings))
         ->Branch  (fn [level ^objects children]
                     (Branch.
                       level
                       (count children)
                       ^objects (arrays/amap #(.maxKey ^ANode %) Object children)
                       nil
                       children
                       settings))]
     (loop [level 1
            nodes (mapv ->Leaf (split keys len Object avg max))]
       (case (count nodes)
         0 (PersistentSortedSet. {} cmp settings)
         1 (PersistentSortedSet. {} cmp nil storage (first nodes) len settings 0)
         (recur (inc level) (mapv #(->Branch level %) (split nodes (count nodes) Object avg max))))))))

(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  ([^Comparator cmp keys]
   (from-sequential cmp keys (Settings.)))
  ([^Comparator cmp keys opts]
   (let [arr (to-array keys)
         _   (arrays/asort arr cmp)
         len (ArrayUtil/distinct cmp arr)]
     (from-sorted-array cmp arr len opts))))

(defn sorted-set-opts
  "Create a set with custom comparator, metadata and settings"
  [opts]
  (PersistentSortedSet.
    (:meta opts)
    ^Comparator (or (:cmp opts) compare)
    (map->settings opts)))

(defn sorted-set-by
  "Create a set with custom comparator."
  ([cmp] (PersistentSortedSet. ^Comparator cmp))
  ([cmp & keys] (from-sequential cmp keys)))

(defn sorted-set
  "Create a set with default comparator."
  ([] (PersistentSortedSet/EMPTY))
  ([& keys] (from-sequential compare keys)))

(defn restore-by
  "Constructs lazily-loaded set from storage, root address and custom comparator.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed"
  ([cmp address ^IStorage storage]
   (restore-by cmp address storage {}))
  ([cmp address ^IStorage storage opts]
   (PersistentSortedSet. nil cmp address storage nil -1 (map->settings opts) 0)))

(defn restore
  "Constructs lazily-loaded set from storage and root address.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed"
  ([address storage]
   (restore-by RT/DEFAULT_COMPARATOR address storage {}))
  ([address ^IStorage storage opts]
   (restore-by RT/DEFAULT_COMPARATOR address storage opts)))

(defn walk-addresses
  "Visit each address used by this set. Usable for cleaning up
   garbage left in storage from previous versions of the set"
  [^PersistentSortedSet set consume-fn]
  (.walkAddresses set consume-fn))

(defn store
  "Store each not-yet-stored node by calling IStorage::store and remembering
   returned address. Incremental, won’t store same node twice on subsequent calls.
   Returns root address. Remember it and use it for restore"
  ([^PersistentSortedSet set]
   (.store set))
  ([^PersistentSortedSet set ^IStorage storage]
   (.store set storage)))

(defn settings [^PersistentSortedSet set]
  (settings->map (.-_settings set)))
