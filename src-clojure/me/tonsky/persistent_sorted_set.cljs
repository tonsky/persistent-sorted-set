(ns ^{:doc
  "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."
  :author "Nikita Prokopov"}
  me.tonsky.persistent-sorted-set
  (:refer-clojure :exclude [iter conj disj sorted-set sorted-set-by])
  (:require
    [me.tonsky.persistent-sorted-set.arrays :as arrays])
  (:require-macros
    [me.tonsky.persistent-sorted-set.arrays :as arrays]))


; B+ tree
; -------

; Leaf:     keys[]     :: array of values

; Node:     pointers[] :: links to children nodes
;           keys[]     :: max value for whole subtree
;                         node.keys[i] == max(node.pointers[i].keys)
; All arrays are 16..32 elements, inclusive

; BTSet:    root       :: Node or Leaf
;           shift      :: path bit-shift of root level, == (depth - 1) * level-shift
;           cnt        :: size of a set, integer, rolling
;           comparator :: comparator used for ordering
;           meta       :: clojure meta map
;           _hash      :: hash code, same as for clojure collections, on-demand, cached

; Path: conceptually a vector of indexes from root to leaf value, but encoded in a single int.
;       E.g. we have path [7 53 11] representing root.pointers[7].pointers[3].keys[11].
;       In our case level-shift is 8, meaning each index will take 8 bits:
;       (7 << 16) | (53 << 8) | 11 = 472331
;       0000 0111   0011 0101   0000 1011

; Iter:     set       :: Set this iterator belongs to
;           left      :: Current path
;           right     :: Right bound path (exclusive)
;           keys      :: Cached ref for keys array for a leaf
;           idx       :: Cached idx in keys array
; Keys and idx are cached for fast iteration inside a leaf"


(def ^:const min-len 16)
(def ^:const max-len 32)
(def ^:private ^:const avg-len (arrays/half (+ max-len min-len)))
(def ^:private ^:const level-shift (->> (range 31 -1 -1)
                                     (filter #(bit-test max-len %))
                                     first
                                     inc))
(def ^:private ^:const path-mask (dec (bit-shift-left 1 level-shift)))
(def ^:private ^:const empty-path 0)

(defn- path-get [path level]
  (bit-and path-mask
           (unsigned-bit-shift-right path level)))

(defn- path-set [path level idx]
  (bit-or path 
          (bit-shift-left idx level)))

(defn- binary-search-l [cmp arr r k]
  (loop [l 0
         r (long r)]
    (if (<= l r)
      (let [m  (arrays/half (+ l r))
            mk (arrays/aget arr m)]
        (if (neg? (cmp mk k))
          (recur (inc m) r)
          (recur l (dec m))))
      l)))

(defn- binary-search-r [cmp arr r k]
  (loop [l 0
         r (long r)]
    (if (<= l r)
      (let [m  (arrays/half (+ l r))
            mk (arrays/aget arr m)]
        (if (pos? (cmp mk k))
          (recur l (dec m))
          (recur (inc m) r)))
      l)))

(defn- lookup-exact [cmp arr key]
  (let [arr-l (arrays/alength arr)
        idx   (binary-search-l cmp arr (dec arr-l) key)]
    (if (and (< idx arr-l)
             (== 0 (cmp (arrays/aget arr idx) key)))
      idx
      -1)))

(defn- lookup-range [cmp arr key]
  (let [arr-l (arrays/alength arr)
        idx   (binary-search-l cmp arr (dec arr-l) key)]
    (if (== idx arr-l)
      -1
      idx)))

;; Array operations

(defn- cut-n-splice [arr cut-from cut-to splice-from splice-to xs]
  (let [xs-l (arrays/alength xs)
        l1   (- splice-from cut-from)
        l2   (- cut-to splice-to)
        l1xs (+ l1 xs-l)
        new-arr (arrays/make-array (+ l1 xs-l l2))]
    (arrays/acopy arr cut-from splice-from new-arr 0)
    (arrays/acopy xs 0 xs-l new-arr l1)
    (arrays/acopy arr splice-to cut-to new-arr l1xs)
    new-arr))

(defn- splice [arr splice-from splice-to xs]
  (cut-n-splice arr 0 (arrays/alength arr) splice-from splice-to xs))

(defn- insert [arr idx xs]
  (cut-n-splice arr 0 (arrays/alength arr) idx idx xs))

(defn- merge-n-split [a1 a2]
  (let [a1-l    (arrays/alength a1)
        a2-l    (arrays/alength a2)
        total-l (+ a1-l a2-l)
        r1-l    (arrays/half total-l)
        r2-l    (- total-l r1-l)
        r1      (arrays/make-array r1-l)
        r2      (arrays/make-array r2-l)]
    (if (<= a1-l r1-l)
      (do
        (arrays/acopy a1 0             a1-l          r1 0)
        (arrays/acopy a2 0             (- r1-l a1-l) r1 a1-l)
        (arrays/acopy a2 (- r1-l a1-l) a2-l          r2 0))
      (do
        (arrays/acopy a1 0    r1-l r1 0)
        (arrays/acopy a1 r1-l a1-l r2 0)
        (arrays/acopy a2 0    a2-l r2 (- a1-l r1-l))))
    (arrays/array r1 r2)))

(defn- ^boolean eq-arr [cmp a1 a1-from a1-to a2 a2-from a2-to]
  (let [len (- a1-to a1-from)]
    (and
      (== len (- a2-to a2-from))
      (loop [i 0]
        (cond
          (== i len)
          true

          (not (== 0 (cmp
                       (arrays/aget a1 (+ i a1-from))
                       (arrays/aget a2 (+ i a2-from)))))
          false
          
          :else
          (recur (inc i)))))))

(defn- check-n-splice [cmp arr from to new-arr]
  (if (eq-arr cmp arr from to new-arr 0 (arrays/alength new-arr))
    arr
    (splice arr from to new-arr)))

(defn- return-array
  "Drop non-nil references and return array of arguments"
  ([a1]
    (arrays/array a1))
  ([a1 a2]
    (if a1
      (if a2
        (arrays/array a1 a2)
        (arrays/array a1))
      (arrays/array a2)))
  ([a1 a2 a3]
    (if a1
      (if a2
        (if a3
          (arrays/array a1 a2 a3)
          (arrays/array a1 a2))
        (if a3
          (arrays/array a1 a3)
          (arrays/array a1)))
      (if a2
        (if a3
          (arrays/array a2 a3)
          (arrays/array a2))
        (arrays/array a3)))))

;;

(defprotocol INode
  (node-lim-key       [_])
  (node-len           [_])
  (node-merge         [_ next])
  (node-merge-n-split [_ next])
  (node-lookup        [_ cmp key])
  (node-conj          [_ cmp key])
  (node-disj          [_ cmp key root? left right]))

(defn- rotate [node root? left right]
  (cond
    ;; root never merges
    root?
    (return-array node)

    ;; enough keys, nothing to merge
    (> (node-len node) min-len)
    (return-array left node right)

    ;; left and this can be merged to one
    (and left (<= (node-len left) min-len))
    (return-array (node-merge left node) right)

    ;; right and this can be merged to one
    (and right (<= (node-len right) min-len))
    (return-array left (node-merge node right))

    ;; left has fewer nodes, redestribute with it
    (and left (or (nil? right)
                  (< (node-len left) (node-len right))))
    (let [nodes (node-merge-n-split left node)]
      (return-array (arrays/aget nodes 0) (arrays/aget nodes 1) right))

    ;; right has fewer nodes, redestribute with it
    :else
    (let [nodes (node-merge-n-split node right)]
      (return-array left (arrays/aget nodes 0) (arrays/aget nodes 1)))))

(deftype Node [keys pointers]
  INode
  (node-lim-key [_]
    (arrays/alast keys))
  
  (node-len [_]
    (arrays/alength keys))
  
  (node-merge [_ next]
    (Node. (arrays/aconcat keys (.-keys next))
           (arrays/aconcat pointers (.-pointers next))))
  
  (node-merge-n-split [_ next]
    (let [ks (merge-n-split keys     (.-keys next))
          ps (merge-n-split pointers (.-pointers next))]
      (return-array (Node. (arrays/aget ks 0) (arrays/aget ps 0))
                    (Node. (arrays/aget ks 1) (arrays/aget ps 1)))))

  (node-lookup [_ cmp key]
    (let [idx (lookup-range cmp keys key)]
      (when-not (== -1 idx)
        (node-lookup (arrays/aget pointers idx) cmp key))))
  
  (node-conj [_ cmp key]
    (let [idx   (binary-search-l cmp keys (- (arrays/alength keys) 2) key)
          nodes (node-conj (arrays/aget pointers idx) cmp key)]
      (when nodes
        (let [new-keys     (check-n-splice cmp keys     idx (inc idx) (arrays/amap node-lim-key nodes))
              new-pointers (splice             pointers idx (inc idx) nodes)]
          (if (<= (arrays/alength new-pointers) max-len)
            ;; ok as is
            (arrays/array (Node. new-keys new-pointers))
            ;; gotta split it up
            (let [middle  (arrays/half (arrays/alength new-pointers))]
              (arrays/array
                (Node. (.slice new-keys     0 middle)
                       (.slice new-pointers 0 middle))
                (Node. (.slice new-keys     middle)
                       (.slice new-pointers middle)))))))))

  (node-disj [_ cmp key root? left right]
    (let [idx (lookup-range cmp keys key)]
      (when-not (== -1 idx) ;; short-circuit, key not here
        (let [child       (arrays/aget pointers idx)
              left-child  (when (>= (dec idx) 0)
                            (arrays/aget pointers (dec idx)))
              right-child (when (< (inc idx) (arrays/alength pointers))
                            (arrays/aget pointers (inc idx)))
              disjned     (node-disj child cmp key false left-child right-child)]
          (when disjned     ;; short-circuit, key not here
            (let [left-idx     (if left-child  (dec idx) idx)
                  right-idx    (if right-child (+ 2 idx) (+ 1 idx))
                  new-keys     (check-n-splice cmp keys     left-idx right-idx (arrays/amap node-lim-key disjned))
                  new-pointers (splice             pointers left-idx right-idx disjned)]
              (rotate (Node. new-keys new-pointers) root? left right))))))))

(deftype Leaf [keys]
  INode
  (node-lim-key [_]
    (arrays/alast keys))
;;   Object
;;   (toString [_] (pr-str* (vec keys)))
  
  (node-len [_]
    (arrays/alength keys))
  
  (node-merge [_ next]
    (Leaf. (arrays/aconcat keys (.-keys next))))
  
  (node-merge-n-split [_ next]
    (let [ks (merge-n-split keys (.-keys next))]
      (return-array (Leaf. (arrays/aget ks 0))
                    (Leaf. (arrays/aget ks 1)))))
  
  (node-lookup [_ cmp key]
    (let [idx (lookup-exact cmp keys key)]
      (when-not (== -1 idx)
        (arrays/aget keys idx))))

  (node-conj [_ cmp key]
    (let [idx    (binary-search-l cmp keys (dec (arrays/alength keys)) key)
          keys-l (arrays/alength keys)]
      (cond
        ;; element already here
        (and (< idx keys-l)
             (== 0 (cmp key (arrays/aget keys idx))))
          nil
      
        ;; splitting
        (== keys-l max-len)
          (let [middle (arrays/half (inc keys-l))]
            (if (> idx middle)
              ;; new key goes to the second half
              (arrays/array
                (Leaf. (.slice keys 0 middle))
                (Leaf. (cut-n-splice keys middle keys-l idx idx (arrays/array key))))
              ;; new key goes to the first half
              (arrays/array
                (Leaf. (cut-n-splice keys 0 middle idx idx (arrays/array key)))
                (Leaf. (.slice keys middle keys-l)))))
       
        ;; ok as is
        :else
          (arrays/array (Leaf. (splice keys idx idx (arrays/array key)))))))
  
  (node-disj [_ cmp key root? left right]
    (let [idx (lookup-exact cmp keys key)]
      (when-not (== -1 idx) ;; key is here
        (let [new-keys (splice keys idx (inc idx) (arrays/array))]
          (rotate (Leaf. new-keys) root? left right))))))

;; BTSet

(declare conj disj btset-iter)

(def ^:private ^:const uninitialized-hash nil)

(deftype BTSet [root shift cnt comparator meta ^:mutable _hash]
  Object
  (toString [this] (pr-str* this))

  ICloneable
  (-clone [_] (BTSet. root shift cnt comparator meta _hash))

  IWithMeta
  (-with-meta [_ new-meta] (BTSet. root shift cnt comparator new-meta _hash))

  IMeta
  (-meta [_] meta)

  IEmptyableCollection
  (-empty [_] (BTSet. (Leaf. (arrays/array)) 0 0 comparator meta uninitialized-hash))

  IEquiv
  (-equiv [this other]
    (and
      (set? other)
      (== cnt (count other))
      (every? #(contains? this %) other)))

  IHash
  (-hash [this] (caching-hash this hash-unordered-coll _hash))

  ICollection
  (-conj [this key] (conj this key comparator))

  ISet
  (-disjoin [this key] (disj this key comparator))

  ILookup 
  (-lookup [_ k]
    (node-lookup root comparator k))
  (-lookup [_ k not-found]
    (or (node-lookup root comparator k) not-found))

  ISeqable
  (-seq [this] (btset-iter this))

  IReduce
  (-reduce [this f]
    (if-let [i (btset-iter this)]
      (-reduce i f)
      (f)))
  (-reduce [this f start]
    (if-let [i (btset-iter this)]
      (-reduce i f start)
      start))
           
  IReversible
  (-rseq [this] (rseq (btset-iter this)))

  ; ISorted
  ; (-sorted-seq [this ascending?])
  ; (-sorted-seq-from [this k ascending?])
  ; (-entry-key [this entry] entry)
  ; (-comparator [this] comparator)

  ICounted
  (-count [_] cnt)

  IEditableCollection
  (-as-transient [this] this)

  ITransientCollection
  (-conj! [this key] (conj this key comparator))
  (-persistent! [this] this)

  ITransientSet
  (-disjoin! [this key] (disj this key comparator))

  IFn
  (-invoke [this k] (-lookup this k))
  (-invoke [this k not-found] (-lookup this k not-found))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "#{" " " "}" opts (seq this))))
  
(defn- keys-for [set path]
  (loop [level (.-shift set)
         node  (.-root set)]
    (if (pos? level)
      (recur (- level level-shift)
             (arrays/aget (.-pointers node)
                   (path-get path level)))
      (.-keys node))))

(defn- alter-btset [set root shift cnt]
  (BTSet. root shift cnt (.-comparator set) (.-meta set) uninitialized-hash))


;; iteration

(defn- -next-path [node path level]
  (let [idx (path-get path level)]
    (if (pos? level)
      ;; inner node
      (let [sub-path (-next-path (arrays/aget (.-pointers node) idx) path (- level level-shift))]
        (if (== -1 sub-path)
          ;; nested node overflow
          (if (< (inc idx) (arrays/alength (.-pointers node)))
            ;; advance current node idx, reset subsequent indexes
            (path-set empty-path level (inc idx))
            ;; current node overflow
            -1)
          ;; keep current idx
          (path-set sub-path level idx)))
      ;; leaf
      (if (< (inc idx) (arrays/alength (.-keys node)))
        ;; advance leaf idx
        (path-set empty-path 0 (inc idx))
        ;; leaf overflow
        -1))))

(defn- next-path
  "Returns path representing next item after `path` in natural traversal order,
   or -1 if end of tree has been reached"
  [set path]
  (-next-path (.-root set) path (.-shift set)))

(defn- -rpath
  "Returns rightmost path possible starting from node and going deeper"
  [node level]
  (loop [node  node
         path  empty-path
         level level]
    (if (pos? level)
      ;; inner node
      (recur (arrays/alast (.-pointers node))
             (path-set path level (dec (arrays/alength (.-pointers node))))
             (- level level-shift))
      ;; leaf
      (path-set path 0 (dec (arrays/alength (.-keys node)))))))

(defn- -prev-path [node path level]
  (let [idx (path-get path level)]
    (if (pos? level)
      ;; inner node
      (let [sub-level (- level level-shift)
            sub-path  (-prev-path (arrays/aget (.-pointers node) idx) path sub-level)]
        (if (== -1 sub-path)
          ;; nested node overflow
          (if (>= (dec idx) 0)
            ;; advance current node idx, reset subsequent indexes
            (let [idx      (dec idx)
                  sub-path (-rpath (arrays/aget (.-pointers node) idx) sub-level)]
              (path-set sub-path level idx))
            ;; current node overflow
            -1)
          ;; keep current idx
          (path-set sub-path level idx)))
      ;; leaf
      (if (>= (dec idx) 0)
        ;; advance leaf idx
        (path-set empty-path 0 (dec idx))
        ;; leaf overflow
        -1))))

(defn- prev-path
  "Returns path representing previous item before `path` in natural traversal order,
   or -1 if `path` was already beginning of a tree"
  [set path]
  (-prev-path (.-root set) path (.-shift set)))

(declare iter riter)

(defn- btset-iter
  "Iterator that represents the whole set"
  [set]
  (when (pos? (node-len (.-root set)))
    (let [left   empty-path
          right  (inc (-rpath (.-root set) (.-shift set)))]
      (iter set left right))))

;; replace with cljs.core/ArrayChunk after https://dev.clojure.org/jira/browse/CLJS-2470
(deftype Chunk [arr off end]
  ICounted
  (-count [_] (- end off))

  IIndexed
  (-nth [this i]
    (aget arr (+ off i)))
  (-nth [this i not-found]
    (if (and (>= i 0) (< i (- end off)))
      (aget arr (+ off i))
      not-found))

  IChunk
  (-drop-first [this]
    (if (== off end)
      (throw (js/Error. "-drop-first of empty chunk"))
      (ArrayChunk. arr (inc off) end)))

  IReduce
  (-reduce [this f]
    (if (== off end)
      (f)
      (-reduce (-drop-first this) f (aget arr off))))
  (-reduce [this f start]
    (loop [val start, n off]
      (if (< n end)
        (let [val' (f val (aget arr n))]
          (if (reduced? val')
            @val'
            (recur val' (inc n))))
        val))))

(defprotocol IIter
  (-copy [this left right]))

(deftype Iter [set left right keys idx]
  IIter
  (-copy [_ l r]
    (Iter. set l r (keys-for set l) (path-get l 0)))

  IEquiv
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] (when keys this))

  ISeq
  (-first [this]
    (when keys
      (arrays/aget keys idx)))

  (-rest [this] (or (-next this) ()))

  INext
  (-next [this]
    (when keys
      (if (< (inc idx) (arrays/alength keys))
        ;; can use cached array to move forward
        (when (< (inc left) right)
          (Iter. set (inc left) right keys (inc idx)))
        (let [left' (next-path set left)]
          (when (and (not= -1 left') (< left' right))
            (-copy this left' right))))))

  IChunkedSeq
  (-chunked-first [this]
    (let [end-idx (if (= (bit-or left path-mask)
                         (bit-or right path-mask))
                    (bit-and right path-mask)
                    (arrays/alength keys))]
      (Chunk. keys idx end-idx)))

  (-chunked-rest [this]
    (or (-chunked-next this) ()))

  IChunkedNext
  (-chunked-next [this]
    (let [left' (next-path set (+ left (- (arrays/alength keys) idx 1)))]
      (when (and (not= -1 left') (< left' right))
        (-copy this left' right))))
           
  IReduce
  (-reduce [this f]
    (if (nil? keys)
      (f)
      (let [first (-first this)]
        (if-some [next (-next this)]
          (-reduce next f first)
          first))))

  (-reduce [this f start]
    (loop [left left
           keys keys
           idx  idx
           acc  start]
      (if (nil? keys)
        acc
        (let [new-acc (f acc (arrays/aget keys idx))]
          (cond
            (reduced? new-acc)
            @new-acc

            (< (inc idx) (arrays/alength keys)) ;; can use cached array to move forward
            (if (< (inc left) right)
              (recur (inc left) keys (inc idx) new-acc)
              new-acc)

            :else
            (let [new-left (next-path set left)]
              (if (and (not (== -1 new-left)) (< new-left right))
                (recur new-left (keys-for set new-left) (path-get new-left 0) new-acc)
                new-acc)))))))

  IReversible
  (-rseq [this]
    (when keys
      (riter set (prev-path set left) (prev-path set right))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

(defn iter [set left right]
  (Iter. set left right (keys-for set left) (path-get left 0)))

;; reverse iteration

(deftype ReverseIter [set left right keys idx]
  IIter
  (-copy [_ l r]
    (ReverseIter. set l r (keys-for set r) (path-get r 0)))

  IEquiv
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] (when keys this))

  ISeq
  (-first [this]
    (when keys
      (arrays/aget keys idx)))

  (-rest [this]  (or (-next this) ()))

  INext
  (-next [this]
    (when keys
      (if (>= (dec idx) 0)
        ;; can use cached array to advance
        (when (> (dec right) left)
          (ReverseIter. set left (dec right) keys (dec idx)))
        (let [right (prev-path set right)]
          (when (and (not= -1 right) (> right left))
            (-copy this left right))))))

  IReversible
  (-rseq [this]
    (when keys
      (let [new-left  (if (== left -1) 0 (next-path set left))
            new-right (next-path set right)
            new-right (if (== new-right -1) (inc right) new-right)]
        (iter set new-left new-right))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

(defn riter [set left right]
  (ReverseIter. set left right (keys-for set right) (path-get right 0)))

;; distance

(defn- -distance [node left right level]
  (let [idx-l (path-get left level)
        idx-r (path-get right level)]
    (if (pos? level)
      ;; inner node
      (if (== idx-l idx-r)
        (-distance (arrays/aget (.-pointers node) idx-l) left right (- level level-shift))
        (loop [level level
               res   (- idx-r idx-l)]
          (if (== 0 level)
            res
            (recur (- level level-shift) (* res avg-len)))))
      (- idx-r idx-l))))

(defn- distance [set path-l path-r]
  (cond
    (== path-l path-r) 0
    (== (inc path-l) path-r) 1
    (== (next-path set path-l) path-r) 1
    :else (-distance (.-root set) path-l path-r (.-shift set))))

(defn est-count [iter]
  (distance (.-set iter) (.-left iter) (.-right iter)))


;; Slicing

(defn- -seek
  "Returns path to first element >= key,
   or -1 if all elements in a set < key"
  [set key comparator]
  (loop [node  (.-root set)
         path  empty-path
         level (.-shift set)]
    (let [keys-l (node-len node)]
      (if (== 0 level)
        (let [keys (.-keys node)
              idx  (binary-search-l comparator keys (dec keys-l) key)]
          (if (== keys-l idx) -1 (path-set path 0 idx)))
        (let [keys (.-keys node)
              idx  (binary-search-l comparator keys (- keys-l 2) key)]
          (recur (arrays/aget (.-pointers node) idx)
                 (path-set path level idx)
                 (- level level-shift)))))))

(defn- -rseek
  "Returns path to the first element that is > key.
   If all elements in a set are <= key, returns `(-rpath set) + 1`.
   It’s a virtual path that is bigger than any path in a tree"
  [set key comparator]
  (loop [node  (.-root set)
         path  empty-path
         level (.-shift set)]
    (let [keys-l (node-len node)]
      (if (== 0 level)
        (let [keys (.-keys node)
              idx  (binary-search-r comparator keys (dec keys-l) key)]
          (path-set path 0 idx))
        (let [keys (.-keys node)
              idx  (binary-search-r comparator keys (- keys-l 2) key)]
          (recur (arrays/aget (.-pointers node) idx)
                 (path-set path level idx)
                 (- level level-shift)))))))

(defn- -slice [set key-from key-to comparator]
  (let [path (-seek set key-from comparator)]
    (when-not (neg? path)
      (let [till-path (-rseek set key-to comparator)]
        (when (> till-path path)
          (Iter. set path till-path (keys-for set path) (path-get path 0)))))))


(defn- arr-map-inplace [f arr]
  (let [len (arrays/alength arr)]
    (loop [i 0]
      (when (< i len)
        (arrays/aset arr i (f (arrays/aget arr i)))
        (recur (inc i))))
    arr))


(defn- arr-partition-approx
  "Splits `arr` into arrays of size between min-len and max-len,
   trying to stick to (min+max)/2"
  [min-len max-len arr]
  (let [chunk-len avg-len
        len       (arrays/alength arr)
        acc       (transient [])]
    (when (pos? len)
      (loop [pos 0]
        (let [rest (- len pos)]
          (cond
            (<= rest max-len)
              (conj! acc (.slice arr pos))
            (>= rest (+ chunk-len min-len))
              (do
                (conj! acc (.slice arr pos (+ pos chunk-len)))
                (recur (+ pos chunk-len)))
            :else
              (let [piece-len (arrays/half rest)]
                (conj! acc (.slice arr pos (+ pos piece-len)))
                (recur (+ pos piece-len)))))))
    (to-array (persistent! acc))))


(defn- sorted-arr-distinct? [arr cmp]
  (let [al (arrays/alength arr)]
    (if (<= al 1)
      true
      (loop [i 1
             p (arrays/aget arr 0)]
        (if (>= i al)
          true
          (let [e (arrays/aget arr i)]
            (if (== 0 (cmp e p))
              false
              (recur (inc i) e))))))))


(defn- sorted-arr-distinct
  "Filter out repetitive values in a sorted array.
   Optimized for no-duplicates case"
  [arr cmp]
  (if (sorted-arr-distinct? arr cmp)
    arr
    (let [al (arrays/alength arr)]
      (loop [acc (transient [(arrays/aget arr 0)])
             i   1
             p   (arrays/aget arr 0)]
        (if (>= i al)
          (into-array (persistent! acc))
          (let [e (arrays/aget arr i)]
            (if (== 0 (cmp e p))
              (recur acc (inc i) e)
              (recur (conj! acc e) (inc i) e))))))))


;; Public interface

(defn conj
  "Analogue to [[clojure.core/conj]] with comparator that overrides the one stored in set."
  [set key cmp]
  (let [roots (node-conj (.-root set) cmp key)]
    (cond
      ;; tree not changed
      (nil? roots)
        set
     
      ;; keeping single root
      (== (arrays/alength roots) 1)
        (alter-btset set
          (arrays/aget roots 0)
          (.-shift set)
          (inc (.-cnt set)))
     
      ;; introducing new root
      :else
        (alter-btset set
          (Node. (arrays/amap node-lim-key roots) roots)
          (+ (.-shift set) level-shift)
          (inc (.-cnt set))))))


(defn disj
  "Analogue to [[clojure.core/disj]] with comparator that overrides the one stored in set."
  [set key cmp]
  (let [new-roots (node-disj (.-root set) cmp key true nil nil)]
    (if (nil? new-roots) ;; nothing changed, key wasn't in the set
      set
      (let [new-root (arrays/aget new-roots 0)]
        (if (and (instance? Node new-root)
                 (== 1 (arrays/alength (.-pointers new-root))))
          
          ;; root has one child, make him new root
          (alter-btset set
            (arrays/aget (.-pointers new-root) 0)
            (- (.-shift set) level-shift)
            (dec (.-cnt set)))
          
          ;; keeping root level
          (alter-btset set
            new-root
            (.-shift set)
            (dec (.-cnt set))))))))


(defn slice
  "An iterator for part of the set with provided boundaries.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([set key-from key-to]
    (-slice set key-from key-to (.-comparator set)))
  ([set key-from key-to comparator]
    (-slice set key-from key-to comparator)))


(defn rslice
  "A reverse iterator for part of the set with provided boundaries.
   `(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([set key]
    (some-> (-slice set key key (.-comparator set)) rseq))
  ([set key-from key-to]
    (some-> (-slice set key-to key-from (.-comparator set)) rseq))
  ([set key-from key-to comparator]
    (some-> (-slice set key-to key-from comparator) rseq)))


(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  [cmp arr]
  (let [leaves (->> arr
                   (arr-partition-approx min-len max-len)
                   (arr-map-inplace #(Leaf. %)))]
    (loop [current-level leaves
           shift 0]
      (case (count current-level)
        0 (BTSet. (Leaf. (arrays/array)) 0 0 cmp nil uninitialized-hash)
        1 (BTSet. (first current-level) shift (arrays/alength arr) cmp nil uninitialized-hash)
        (recur (->> current-level
                    (arr-partition-approx min-len max-len)
                    (arr-map-inplace #(Node. (arrays/amap node-lim-key %) %)))
               (+ shift level-shift))))))


(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  [cmp seq]
  (let [arr (-> (into-array seq) (arrays/asort cmp) (sorted-arr-distinct cmp))]
    (from-sorted-array cmp arr)))


(defn sorted-set-by
  ([cmp] (BTSet. (Leaf. (arrays/array)) 0 0 cmp nil uninitialized-hash))
  ([cmp & keys] (from-sequential cmp keys)))


(defn sorted-set
  ([] (sorted-set-by compare))
  ([& keys] (from-sequential compare keys)))