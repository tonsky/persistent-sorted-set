(ns me.tonsky.persistent-sorted-set.test.core
  (:require
    [me.tonsky.persistent-sorted-set :as set]
    [clojure.test :as t :refer [is are deftest testing]])
  #?(:clj
      (:import [clojure.lang IReduce])))

#?(:clj (set! *warn-on-reflection* true))

(def iters 5)

;; confirm that clj's use of sorted set works as intended.
;; allow for [:foo nil] to glob [:foo *]; data will never be inserted
;; w/ nil, but slice/subseq elements will.

(defn cmp [x y]
  (if (and x y)
    (compare x y)
    0))

(defn cmp-s [[x0 x1] [y0 y1]]
  (let [c0 (cmp x0 y0)
        c1 (cmp x1 y1)]
    (cond
      (= c0 0) c1
      (< c0 0) -1
      (> c0 0)  1)))

(deftest semantic-test-btset-by
  (let [e0 (set/sorted-set-by cmp-s)
        ds [[:a :b] [:b :x] [:b :q] [:a :d]]
        e1 (reduce conj e0 ds)]
    (is (= (count ds)        (count (seq e1))))
    (is (= (vec (seq e1))    (vec (set/slice e1 [nil nil] [nil nil])))) ; * *
    (is (= [[:a :b] [:a :d]] (vec (set/slice e1 [:a nil]  [:a nil] )))) ; :a *
    (is (= [[:b :q]]         (vec (set/slice e1 [:b :q]   [:b :q]  )))) ; :b :q (specific)
    (is (= [[:a :d] [:b :q]] (vec (set/slice e1 [:a :d]   [:b :q]  )))) ; matching subrange
    (is (= [[:a :d] [:b :q]] (vec (set/slice e1 [:a :c]   [:b :r]  )))) ; non-matching subrange
    (is (= [[:b :x]]         (vec (set/slice e1 [:b :r]   [:c nil] )))) ; non-matching -> out of range
    (is (= []                (vec (set/slice e1 [:c nil]  [:c nil] )))) ; totally out of range
    ))

(defn irange [from to]
  (if (< from to)
    (range from (inc to))
    (range from (dec to) -1)))

(deftest test-slice
  (dotimes [i iters]
    (testing "straight 3 layers"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (set/slice s from to))
          nil    nil    (irange 0 5000)
          
          -1     nil    (irange 0 5000)
          0      nil    (irange 0 5000)
          0.5    nil    (irange 1 5000)
          1      nil    (irange 1 5000)
          4999   nil    [4999 5000]
          4999.5 nil    [5000]
          5000   nil    [5000]
          5000.5 nil    nil

          nil    -1     nil
          nil    0      [0]
          nil    0.5    [0]
          nil    1      [0 1]
          nil    4999   (irange 0 4999)
          nil    4999.5 (irange 0 4999)
          nil    5000   (irange 0 5000)
          nil    5001   (irange 0 5000)

          -2     -1     nil
          -1     5001   (irange 0 5000)
          0      5000   (irange 0 5000)
          0.5    4999.5 (irange 1 4999)
          2499.5 2500.5 [2500]
          2500   2500   [2500]
          2500.1 2500.9 nil
          5001   5002   nil)))

    (testing "straight 1 layer, leaf == root"
      (let [s (into (set/sorted-set) (shuffle (irange 0 10)))]
        (are [from to expected] (= expected (set/slice s from to))
          nil  nil  (irange 0 10)
          
          -1   nil  (irange 0 10)
          0    nil  (irange 0 10)
          0.5  nil  (irange 1 10)
          1    nil  (irange 1 10)
          9    nil  [9 10]
          9.5  nil  [10]
          10   nil  [10]
          10.5 nil  nil
          
          nil -1   nil
          nil 0    [0]
          nil 0.5  [0]
          nil 1    [0 1]
          nil 9    (irange 0 9)
          nil 9.5  (irange 0 9)
          nil 10   (irange 0 10)
          nil 11   (irange 0 10)

          -2   -1  nil
          -1   10  (irange 0 10)
          0    10  (irange 0 10)
          0.5  9.5 (irange 1 9)
          4.5  5.5 [5]
          5    5   [5]
          5.1  5.9 nil
          11   12  nil)))

    (testing "reverse 3 layers"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (set/rslice s from to))
          nil    nil    (irange 5000 0)
          
          5001   nil    (irange 5000 0)
          5000   nil    (irange 5000 0)
          4999.5 nil    (irange 4999 0)
          4999   nil    (irange 4999 0)
          1      nil    [1 0]
          0.5    nil    [0]
          0      nil    [0]
          -1     nil    nil
          
          nil    5001   nil
          nil    5000   [5000]
          nil    4999.5 [5000]
          nil    4999   [5000 4999]
          nil    1      (irange 5000 1)
          nil    0.5    (irange 5000 1)
          nil    0      (irange 5000 0)
          nil    -1     (irange 5000 0)

          5002   5001   nil
          5001   -1     (irange 5000 0)
          5000   0      (irange 5000 0)
          4999.5 0.5    (irange 4999 1)
          2500.5 2499.5 [2500]
          2500   2500   [2500]
          2500.9 2500.1 nil
          -1     -2     nil)))

    (testing "reverse 1 layer, leaf == root"
      (let [s (into (set/sorted-set) (shuffle (irange 0 10)))]
        (are [from to expected] (= expected (set/rslice s from to))
          nil nil (irange 10 0)
          
          11  nil (irange 10 0)
          10  nil (irange 10 0)
          9.5 nil (irange 9 0)
          9   nil (irange 9 0)
          1   nil [1 0]
          0.5 nil [0]
          0   nil [0]
          -1  nil nil
          
          nil 11  nil
          nil 10  [10]
          nil 9.5 [10]
          nil 9   [10 9]
          nil 1   (irange 10 1)
          nil 0.5 (irange 10 1)
          nil 0   (irange 10 0)
          nil -1  (irange 10 0)

          12  11  nil
          11  -1  (irange 10 0)
          10  0   (irange 10 0)
          9.5 0.5 (irange 9 1)
          5.5 4.5 [5]
          5   5   [5]
          5.9 5.1 nil
          -1  -2  nil)))

    (testing "seq-rseq equivalence"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to] (= (set/slice s from to) (some-> (set/slice s from to) (rseq) (reverse)))
          -1     nil
          0      nil
          2500   nil
          5000   nil
          5001   nil
          
          nil    -1
          nil    0     
          nil    1     
          nil    2500
          nil    5000
          nil    5001  
          
          nil    nil
 
          -1     5001
          0      5000  
          1      4999
          2500   2500
          2500.1 2500.9)))

    (testing "rseq-seq equivalence"
      (let [s (into (set/sorted-set) (shuffle (irange 0 5000)))]
        (are [from to] (= (set/rslice s from to) (some-> (set/rslice s from to) (rseq) (reverse)))
          -1     nil
          0      nil
          2500   nil
          5000   nil
          5001   nil
          
          nil    -1
          nil    0     
          nil    1     
          nil    2500
          nil    5000
          nil    5001  
          
          nil    nil

          5001   -1    
          5000   0       
          4999   1     
          2500   2500  
          2500.9 2500.1)))

    (testing "Slice with equal elements"
      (let [cmp10 (fn [a b] (compare (quot a 10) (quot b 10)))
            s10   (reduce #(set/conj %1 %2 compare) (set/sorted-set-by cmp10) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (set/slice s10 from to))
          30 30      (irange 30 39)
          130 4970   (irange 130 4979)
          -100 6000  (irange 0 5000))
        (are [from to expected] (= expected (set/rslice s10 from to))
          30 30      (irange 39 30)
          4970 130   (irange 4979 130)
          6000 -100  (irange 5000 0)))

      (let [cmp100 (fn [a b] (compare (quot a 100) (quot b 100)))
            s100   (reduce #(set/conj %1 %2 compare) (set/sorted-set-by cmp100) (shuffle (irange 0 5000)))]
        (are [from to expected] (= expected (set/slice s100 from to))
          30  30     (irange 0 99)
          2550 2550  (irange 2500 2599)
          130 4850   (irange 100 4899)
          -100 6000  (irange 0 5000))
        (are [from to expected] (= expected (set/rslice s100 from to))
          30 30      (irange 99 0)
          2550 2550  (irange 2599 2500)
          4850 130   (irange 4899 100)
          6000 -100  (irange 5000 0))))
    ))

(defn ireduce
  ([f coll] (#?(:clj .reduce :cljs -reduce) ^IReduce coll f))
  ([f val coll] (#?(:clj .reduce :cljs -reduce) ^IReduce coll f val)))

(defn reduce-chunked [f val coll]
  (if-some [s (seq coll)]
    (if (chunked-seq? s)
      (recur f (#?(:clj .reduce :cljs -reduce) (chunk-first s) f val) (chunk-next s))
      (recur f (f val (first s)) (next s)))
    val))

(deftest test-reduces
  (testing "IReduced"
    (testing "Empty"
      (let [s (set/sorted-set)]
        (is (= 0 (ireduce + s)))
        (is (= 0 (ireduce + 0 s)))))

    (testing "~3 layers"
      (let [s (into (set/sorted-set) (irange 0 5000))]
        (is (= 12502500 (ireduce +   s)))
        (is (= 12502500 (ireduce + 0 s)))
        (is (= 12502500 (ireduce +   (seq s))))
        (is (= 12502500 (ireduce + 0 (seq s))))
        (is (= 7502500  (ireduce +   (set/slice s 1000 4000))))
        (is (= 7502500  (ireduce + 0 (set/slice s 1000 4000))))
        #?@(:clj [(is (= 12502500 (ireduce +   (rseq s))))
                  (is (= 12502500 (ireduce + 0 (rseq s))))
                  (is (= 7502500  (ireduce +   (set/rslice s 4000 1000))))
                  (is (= 7502500  (ireduce + 0 (set/rslice s 4000 1000))))])))

    (testing "~1 layer"
      (let [s (into (set/sorted-set) (irange 0 10))]
        (is (= 55 (ireduce +   s)))
        (is (= 55 (ireduce + 0 s)))
        (is (= 55 (ireduce +   (seq s))))
        (is (= 55 (ireduce + 0 (seq s))))
        (is (= 35 (ireduce +   (set/slice s 2 8))))
        (is (= 35 (ireduce + 0 (set/slice s 2 8))))
        #?@(:clj [(is (= 55 (ireduce +   (rseq s))))
                  (is (= 55 (ireduce + 0 (rseq s))))
                  (is (= 35 (ireduce +   (set/rslice s 8 2))))
                  (is (= 35 (ireduce + 0 (set/rslice s 8 2))))]))))

  (testing "IChunkedSeq"
    (testing "~3 layers"
      (let [s (into (set/sorted-set) (irange 0 5000))]
        (is (= 12502500 (reduce-chunked + 0 s)))
        (is (= 7502500  (reduce-chunked + 0 (set/slice s 1000 4000))))
        (is (= 12502500 (reduce-chunked + 0 (rseq s))))
        (is (= 7502500  (reduce-chunked + 0 (set/rslice s 4000 1000))))))

    (testing "~1 layer"
      (let [s (into (set/sorted-set) (irange 0 10))]
        (is (= 55 (reduce-chunked + 0 s)))
        (is (= 35 (reduce-chunked + 0 (set/slice s 2 8))))
        (is (= 55 (reduce-chunked + 0 (rseq s))))
        (is (= 35 (reduce-chunked + 0 (set/rslice s 8 2))))))))


#?(:clj
    (deftest iter-over-transient
      (let [set (transient (into (set/sorted-set) (range 100)))
            seq (seq set)]
        (conj! set 100)
        (is (thrown-with-msg? Exception #"iterating and mutating" (first seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (next seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (reduce + seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (reduce + 0 seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (chunk-first seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (chunk-next seq)))
        (is (thrown-with-msg? Exception #"iterating and mutating" (.iterator ^Iterable seq))))))

(deftest seek-for-seq-test
  (let [size 1000
        set (apply set/sorted-set (range size))
        set-seq (seq set)
        set-rseq (rseq set)]
    (testing "simple seek for seq testing"
      (doseq [seek-loc (map #(* 100 %) (range 10))]
        (is (= seek-loc (first (set/seek set-seq seek-loc)))))
      (doseq [seek-loc (map #(* 100 %) (range 10))]
        (is (= seek-loc (first (set/seek set-rseq seek-loc))))))

    (testing "multiple seek testing"
      (is (= 500 (-> set-seq (set/seek 250) (set/seek 500) first)))
      (is (= 500 (-> set-rseq (set/seek 750) (set/seek 500) first))))

    (testing "normal seq behaviour after seek"
      (is (= (range 500 1000) (-> set-seq (set/seek 250) (set/seek 500))))
      (is (= (range 999 499 -1) (-> set-seq (set/seek 250) (set/seek 500) rseq)))
      (is (= (range 500 -1 -1) (-> set-rseq (set/seek 750) (set/seek 500))))
      (is (= (range 0 501) (-> set-rseq (set/seek 750) (set/seek 500) rseq))))

    #?(:clj
       (testing "nil behaviour"
         (is (thrown-with-msg? Exception #"seek can't be called with a nil key!" (set/seek set-seq nil)))))

    (testing "slicing together with seek"
      (is (= (range 5000 7501) (-> (set/slice (apply set/sorted-set (range 10000)) 2500 7500)
                                   (set/seek 5000))))
      (is (= (list 7500) (-> (set/slice (apply set/sorted-set (range 10000)) 2500 7500)
                             (set/seek 5000)
                             (set/seek 7500))))
      (is (= (range 5000 2499 -1) (-> (set/rslice (apply set/sorted-set (range 10000)) 7500 2500)
                                      (set/seek 5000))))
      (is (= (list 2500) (-> (set/rslice (apply set/sorted-set (range 10000)) 7500 2500)
                             (set/seek 5000)
                             (set/seek 2500)))))))