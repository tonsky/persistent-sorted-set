(ns ^{:author "Nikita Prokopov & Christian Weilbach"
      :doc "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."}
  me.tonsky.persistent-sorted-set
  (:refer-clojure :exclude [conj disj sorted-set sorted-set-by])
  (:require
   [me.tonsky.persistent-sorted-set.arrays :as arrays])
  (:import
    [java.util Comparator Arrays]
    [me.tonsky.persistent_sorted_set PersistentSortedSet Leaf Node Edit ArrayUtil StorageBackend Edit]))


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

(def null-storage
  (proxy [StorageBackend] []
    (hitCache [node] nil)
    (load [node] nil)
    (store [node children] nil)))

(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([cmp keys]
   (from-sorted-array cmp keys null-storage))
  ([cmp keys storage]
   (from-sorted-array cmp keys (arrays/alength keys) storage))
  ([cmp keys len storage]
   (let [max    PersistentSortedSet/MAX_LEN
         avg    (quot (+ PersistentSortedSet/MIN_LEN max) 2)
         edit   (Edit. false)
         ->Leaf (fn [keys]
                  (Leaf. storage keys (count keys) edit))
         ->Node (fn [children]
                  (Node. storage
                    (arrays/amap #(.maxKey ^Leaf %) Object children)
                    children (count children) edit))]
     (loop [nodes (mapv ->Leaf (split keys len Object avg max))]
       (case (count nodes)
         0 (PersistentSortedSet. cmp storage)
         1 (PersistentSortedSet. {} cmp (first nodes) len edit 0 storage)
         (recur (mapv ->Node (split nodes (count nodes) Leaf avg max))))))))


(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  ([^Comparator cmp keys]
   (from-sequential cmp keys null-storage))
  ([^Comparator cmp keys storage]
   (let [arr (to-array keys)
         _   (arrays/asort arr cmp)
         len (ArrayUtil/distinct cmp arr)]
     (from-sorted-array cmp arr len storage))))


(defn sorted-set-by
  "Create a set with custom comparator."
  ([cmp] (sorted-set-by cmp null-storage))
  ([cmp storage] (PersistentSortedSet. ^Comparator cmp storage))
  ([cmp storage & keys] (from-sequential cmp keys storage)))


(defn sorted-set
  "Create a set with default comparator."
  ([] (PersistentSortedSet. compare null-storage))
  ([storage] (PersistentSortedSet. compare storage))
  ([storage & keys] (from-sequential compare keys storage)))


;; code for durability

(defprotocol FlushNode
  (-flush [n]))

(extend-protocol FlushNode
  Leaf
  (-flush [n] n)
  Node
  (-flush [n]
    (if (not (.get (.-_isDirty n)))
      n
      (let [_write (.-_write n)
            _ (.lock _write)
            storage (.-_storage n)
            children (into-array Leaf (map #(-flush %) (.-_children n)))
            address (.store storage n children)]
        (set! (.-_address n) address)
        (.set (.-_isDirty n) false)
        (.unlock _write)
        n))))

(defprotocol NodeToMap
  (-to-map [this]))

(extend-protocol NodeToMap
  Leaf
  (-to-map [this]
    {:type ::leaf
     :version 1
     :len (.-_len this)
     :keys (vec (.-_keys this))})
  Node
  (-to-map [this]
    {:type ::node
     :version 1
     :len (.-_len this)
     :keys (vec (.-_keys this))
     :address (.-_address this)}))

(defn map->node [storage m]
  (let [{:keys [type len keys address]} m]
    (if (= type ::node)
      (Node. storage (into-array Object keys) len (Edit. false) address)
      (Leaf. storage (into-array Object keys) len (Edit. false)))))

(defn mark
  "Return a set of all addresses reachable from gc-roots, corresponding to the
  mark phase of a garbage collector."
  ([gc-roots]
   (loop [to-visit gc-roots
          visited  #{}]
     (if-let [to-visit (seq to-visit)]
       (let [[^Leaf node & r]   to-visit
             [children address] (when (= (type node) Node)
                                  (.lock (.-_write node))
                                  (.ensureChildren node)
                                  (let [children (.-_children node)
                                        address  (.-_address node)]
                                    (.unlock (.-_write node))
                                    [children address]))]
         (if address ;; is written Node
           (recur (into r children)
                  (clojure.core/conj visited address))
           (recur r visited)))
       visited))))

(comment

  (require '[konserve.core :as k]
           '[hasch.core :refer [uuid]]
           '[konserve.filestore :refer [new-fs-store]]
           '[clojure.core.cache.wrapped :as wrapped]
           '[clojure.data :as data]
           '[clojure.core.async :as async])

  (def fs-store (async/<!! (new-fs-store "/tmp/pss-store/")))

  (def cache (wrapped/lru-cache-factory {} :threshold 10))

  (add-watch cache :free-memory
             (fn [_ _ old new]
               (let [[delta _ _] (data/diff (set (keys old)) (set (keys new)))]
                 (doseq [^me.tonsky.persistent_sorted_set.Node node delta]
                   (let [_write (.-_write node)]
                     (.lock _write)
                     (println "freeing node: " (.-_address node) (count old) (count new))
                     (.set (.-_isLoaded node) false)
                     (set! (.-_children node) nil)
                     #_(let [children ]
                       (doseq [i (range (alength children))]
                         (aset children i nil)))
                     (.unlock _write))))))

  (def fs-storage
    (proxy [StorageBackend] []
      (hitCache [node]
        #_(println "hitting " (.-_address node))
        (wrapped/hit cache node)
        nil)
      (load [node]
        (let [address (.-_address node)
              new-children (mapv #(map->node this %)
                                 (async/<!! (k/get fs-store address)))]
          (println "loading " address)
          (set! (.-_children node) (into-array Leaf new-children))
          #_(doseq [i (range (alength children))]
            (aset children i (get new-children i)))
          (wrapped/miss cache node nil)
          nil))
      (store [node children]
        (let [children-as-maps (mapv (fn [n] (-to-map n)) children)
              address          (uuid)]
          (wrapped/miss cache node nil)
          #_(println "storing " address)
          (async/<!! (k/assoc fs-store address children-as-maps))
          address))))

  ;; 1. create memory tree
  (def mem-set (apply (partial sorted-set fs-storage) (range 50000)))

  (def new-mem-set (into mem-set (range 50000 100000)))

  ;; 2. flush to store
  (set! (.-_root mem-set)
        (-flush (.-_root mem-set)))

  (set! (.-_root new-mem-set)
        (-flush (.-_root new-mem-set)))

  (mark #{(.-_root new-mem-set)})


  ;; 3. access tree by printing and test loading from storage
  (println
   (.str
    (.-_root mem-set)
    1))

  (count @cache)

  (count (take 100 mem-set))

  (count (take-last 100 new-mem-set))

  ;; playground
  (.-_address (.-_root mem-set))

  (seq (.-_children (.-_root mem-set)))

  (.-_isLoaded (.-_root mem-set))

  (.set (.-_isLoaded (.-_root mem-set)) false)

  )
