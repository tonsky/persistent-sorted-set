(ns ^{:doc
      "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."
      :author "Nikita Prokopov"}
 me.tonsky.persistent-sorted-set
  (:refer-clojure :exclude [conj disj sorted-set sorted-set-by])
  (:require
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [me.tonsky.persistent-sorted-set.impl :as impl :refer [BTSet]]
   [me.tonsky.persistent-sorted-set.protocol :refer [IStorage] :as protocol])
  (:require-macros
   [me.tonsky.persistent-sorted-set.arrays :as arrays]))

(def conj impl/conj)
(def disj impl/disj)

(defn slice
  "An iterator for part of the set with provided boundaries.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^BTSet set key-from key-to]
   (impl/-slice set key-from key-to (.-comparator set)))
  ([^BTSet set key-from key-to comparator]
   ;; (js/console.trace)
   (impl/-slice set key-from key-to comparator)))


(defn rslice
  "A reverse iterator for part of the set with provided boundaries.
   `(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^BTSet set key]
   (some-> (impl/-slice set key key (.-comparator set)) rseq))
  ([^BTSet set key-from key-to]
   (some-> (impl/-slice set key-to key-from (.-comparator set)) rseq))
  ([^BTSet set key-from key-to comparator]
   (some-> (impl/-slice set key-to key-from comparator) rseq)))


(defn seek
  "An efficient way to seek to a specific key in a seq (either returned by [[clojure.core.seq]] or a slice.)
  `(seek (seq set) to)` returns iterator for all Xs where to <= X.
  Optionally pass in comparator that will override the one that set uses."
  ([seq to]
   (impl/-seek seq to))
  ([seq to cmp]
   (impl/-seek seq to cmp)))


(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([cmp arr]
   (from-sorted-array cmp arr (arrays/alength arr) {}))
  ([cmp arr _len]
   (from-sorted-array cmp arr _len {}))
  ([cmp arr _len opts]
   (let [leaves (->> arr
                     (impl/arr-partition-approx impl/min-len impl/max-len)
                     (impl/arr-map-inplace #(impl/Leaf. %)))
         storage (:storage opts)]
     (loop [current-level leaves
            shift 0]
       (case (count current-level)
         0 (impl/BTSet. storage (impl/Leaf. (arrays/array)) 0 0 cmp nil
                        impl/uninitialized-hash impl/uninitialized-address)
         1 (impl/BTSet. storage (first current-level) shift (arrays/alength arr) cmp nil
                        impl/uninitialized-hash impl/uninitialized-address)
         (recur
          (->> current-level
               (impl/arr-partition-approx impl/min-len impl/max-len)
               (impl/arr-map-inplace #(impl/Node. (arrays/amap impl/node-lim-key %) % nil)))
          (inc shift)))))))

(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  [cmp seq]
  (let [arr (-> (into-array seq) (arrays/asort cmp) (impl/sorted-arr-distinct cmp))]
    (from-sorted-array cmp arr)))

(defn sorted-set*
  "Create a set with custom comparator, metadata and settings"
  [opts]
  (impl/BTSet. (:storage opts) (impl/Leaf. (arrays/array)) 0 0 (or (:cmp opts) compare) (:meta opts)
               impl/uninitialized-hash impl/uninitialized-address))

(defn sorted-set-by
  ([cmp] (impl/BTSet. nil (impl/Leaf. (arrays/array)) 0 0 cmp nil
                      impl/uninitialized-hash impl/uninitialized-address))
  ([cmp & keys] (from-sequential cmp keys)))

(defn sorted-set
  ([] (sorted-set-by compare))
  ([& keys] (from-sequential compare keys)))

(defn restore-by
  "Constructs lazily-loaded set from storage, root address and custom comparator.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed"
  ([cmp address ^IStorage storage]
   (restore-by cmp address storage {}))
  ([cmp address ^IStorage storage {:keys [set-metadata]}]
   (when-let [root (protocol/restore storage address)]
     (impl/BTSet. storage root
                                (:shift set-metadata)
                                (:count set-metadata)
                                cmp nil impl/uninitialized-hash address))))

(defn restore
  "Constructs lazily-loaded set from storage and root address.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed"
  ([address storage]
   (restore-by compare address storage {}))
  ([address ^IStorage storage opts]
   (restore-by compare address storage opts)))

(defn store
  "Store each not-yet-stored node by calling IStorage::store and remembering
   returned address. Incremental, won’t store same node twice on subsequent calls.
   Returns root address. Remember it and use it for restore"
  [^BTSet set ^IStorage storage]
  (impl/store set storage))

(defn settings [set]
  {:branching-factor impl/max-len
   :ref-type :strong})
