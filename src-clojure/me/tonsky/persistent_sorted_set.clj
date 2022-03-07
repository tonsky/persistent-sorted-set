(ns ^{:author "Nikita Prokopov"
      :doc "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."}
  me.tonsky.persistent-sorted-set
  (:refer-clojure :exclude [conj disj sorted-set sorted-set-by])
  (:require
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [konserve.core :as k]
   [hasch.core :refer [uuid]])
  (:import
    [java.util Comparator Arrays]
    [me.tonsky.persistent_sorted_set PersistentSortedSet Leaf Node Edit ArrayUtil Loader]))


(defn conj
  "Analogue to [[clojure.core/conj]] but with comparator that overrides the one stored in set."
  [set key cmp]
  (.cons ^PersistentSortedSet set key cmp))


(defn disj
  "Analogue to [[clojure.core/disj]] with comparator that overrides the one stored in set."
  [set key cmp]
  (.disjoin ^PersistentSortedSet set key cmp))


(defn slice
  "An iterator for part of the set with provided boundaries.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   `(slice set from nil)` returns iterator for all Xs where X >= from.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([set from to]
    (.slice ^PersistentSortedSet set from to))
  ([set from to cmp]
    (.slice ^PersistentSortedSet set from to ^Comparator cmp)))


(defn rslice
  "A reverse iterator for part of the set with provided boundaries.
   `(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   `(rslice set from nil)` returns backwards iterator for all Xs where X <= from.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([set from to]
    (.rslice ^PersistentSortedSet set from to))
  ([set from to cmp]
    (.rslice ^PersistentSortedSet set from to ^Comparator cmp)))


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
  ([cmp keys loader]
   (from-sorted-array cmp keys (arrays/alength keys) loader))
  ([cmp keys len loader]
   (let [max    PersistentSortedSet/MAX_LEN
         avg    (quot (+ PersistentSortedSet/MIN_LEN max) 2)
         edit   (Edit. false)
         ->Leaf (fn [keys]
                  (Leaf. loader keys (count keys) edit))
         ->Node (fn [children]
                  (Node. loader
                    (arrays/amap #(.maxKey ^Leaf %) Object children)
                    children (count children) edit))]
     (loop [nodes (mapv ->Leaf (split keys len Object avg max))]
       (case (count nodes)
         0 (PersistentSortedSet. cmp loader)
         1 (PersistentSortedSet. {} cmp (first nodes) len edit 0, loader)
         (recur (mapv ->Node (split nodes (count nodes) Leaf avg max))))))))


(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  [^Comparator cmp keys loader]
  (let [arr (to-array keys)
        _   (arrays/asort arr cmp)
        len (ArrayUtil/distinct cmp arr)]
    (from-sorted-array cmp arr len loader)))


#_(defn sorted-set-by
  "Create a set with custom comparator."
  ([cmp] (PersistentSortedSet. ^Comparator cmp))
  ([cmp & keys] (from-sequential cmp keys)))


(defn sorted-set
  "Create a set with default comparator."
  ([loader] (PersistentSortedSet/EMPTY))
  ([loader & keys] (from-sequential compare keys loader)))



(defn set-flush [n]
  ;; is a leaf
  (if (not (instance? Node n))
    (let [children (vec (.-_keys n))
          address (uuid)]
      (println "write leaf at " address " : " children)
      (set! (.-_address n) address)
      (set! (.-_is_dirty n) false)
      n)
    (let [children (mapv set-flush (.-_children n))
          address (uuid)]
      (println "write node at " address " : " children)
      (set! (.-_address n) address)
      (set! (.-_is_dirty n) false)
      ;; (set! (.-_keys n) nil)
      n)))

(comment

  (def print-loader
    (proxy [Loader] []
      (load [address]
        (println "loading " address)
        (make-array Leaf 0))))

  (println
   (.str
    (.-_root
     (apply (partial sorted-set print-loader) (range 500)))
    1))

  (instance? Node
             (.-_root
              (apply sorted-set (range 100))))

  (set-flush (.-_root
              (apply sorted-set (range 100)))))
