(ns me.tonsky.persistent-sorted-set.test-stress
  (:require
    [me.tonsky.persistent-sorted-set :as set]
    #?(:clj [me.tonsky.persistent-sorted-set.test-storage :as test-storage])
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
        :clj  [clojure.test :as t :refer        [is are deftest testing]]))
  #?(:clj
      (:import [clojure.lang IReduce])))

(def iters 5)

(defn into-via-doseq [to from]
  (let [res (transient [])]
    (doseq [x from]  ;; checking chunked iter
      (conj! res x))
    (persistent! res)))

(deftest stresstest-btset
  (println "[ TEST ] stresstest-btset")
  (dotimes [i iters]
    (let [size      10000
          xs        (vec (repeatedly (+ 1 (rand-int size)) #(rand-int size)))
          xs-sorted (vec (distinct (sort xs)))
          rm        (vec (repeatedly (rand-int (* size 5)) #(rand-nth xs)))
          full-rm   (shuffle (concat xs rm))
          xs-rm     (reduce disj (into (sorted-set) xs) rm)]
      (doseq [[method set0] [["conj" (into (set/sorted-set) xs)]
                             ["bulk" (apply set/sorted-set xs)]
                             #?(:clj ["lazy" (test-storage/roundtrip (into (set/sorted-set) xs))])]
              :let [set1 (reduce disj set0 rm)
                    set2 (persistent! (reduce disj (transient set0) rm))
                    set3 (reduce disj set0 full-rm)
                    set4 (persistent! (reduce disj (transient set0) full-rm))]]
        (println "Iter:" (str (inc i)  "/" iters)
          "set:" method 
          "adds:" (str (count xs) " (" (count xs-sorted) " distinct),")
          "removals:" (str (count rm) " (down to " (count xs-rm) ")"))
        (testing method
          (testing "conj, seq"
            (is (= (vec set0) xs-sorted)))
          (testing "eq"
            (is (= set0 (set xs-sorted)) xs-sorted))
          (testing "count"
            (is (= (count set0) (count xs-sorted))))
          (testing "doseq"
            (is (= (into-via-doseq [] set0) xs-sorted)))
          (testing "disj"
            (is (= (vec set1) (vec xs-rm)))
            (is (= (count set1) (count xs-rm)))
            (is (= set1 xs-rm)))
          (testing "disj transient"
            (is (= (vec set2) (vec xs-rm)))
            (is (= (count set2) (count xs-rm)))
            (is (= set2 xs-rm)))
          (testing "full disj"
            (is (= set3 #{}))
            (is (= set4 #{})))))))
        
  (println "[ DONE ] stresstest-btset"))

(deftest stresstest-slice
  (println "[ TEST ] stresstest-slice")
  (dotimes [i iters]
    (let [xs        (repeatedly (+ 1 (rand-int 20000)) #(rand-int 20000))
          xs-sorted (distinct (sort xs))
          [from to] (sort [(- 10000 (rand-int 20000)) (+ 10000 (rand-int 20000))])
          expected  (filter #(<= from % to) xs-sorted)]
      (doseq [[method set] [["conj" (into (set/sorted-set) xs)]
                            #?(:clj ["lazy" (test-storage/roundtrip (into (set/sorted-set) xs))])]
              :let [set-range (set/slice set from to)]]
        (println
          "Iter:" (str (inc i) "/" iters)
          "set:" method
          "from:" (count xs-sorted) "elements"
          "down to:" (count expected))
        (testing method
          (testing (str "from " from " to " to)
            (is (= (vec set-range) (vec (seq set-range)))) ;; checking IReduce on BTSetIter
            (is (= (vec set-range) expected))
            (is (= (into-via-doseq [] set-range) expected))
            (is (= (vec (rseq set-range)) (reverse expected)))
            (is (= (vec (rseq (rseq set-range))) expected)))))))
            
  (println "[ DONE ] stresstest-slice"))

(deftest test-overflow
  (println "[ TEST ] test-overflow")
  (let [s (into (set/sorted-set) (range 4000000))]
    (is (= 10 (count (take 10 s)))))
  (println "[ DONE ] test-overflow"))