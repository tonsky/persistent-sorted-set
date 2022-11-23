(ns me.tonsky.persistent-sorted-set.test.stress
  (:require
    [me.tonsky.persistent-sorted-set :as set]
    #?(:clj [me.tonsky.persistent-sorted-set.test.storage :as storage])
    [clojure.test :as t :refer [is are deftest testing]]))

(def iters 100)

(defn into-via-doseq [to from]
  (let [res (transient [])]
    (doseq [x from]  ;; checking chunked iter
      (conj! res x))
    (persistent! res)))

(deftest stresstest-btset
  (println "  testing stresstest-btset...")
  (dotimes [i iters]
    (let [size      10000
          xs        (vec (repeatedly (+ 1 (rand-int size)) #(rand-int size)))
          xs-sorted (vec (distinct (sort xs)))
          rm        (vec (repeatedly (rand-int (* size 5)) #(rand-nth xs)))
          full-rm   (shuffle (concat xs rm))
          xs-rm     (reduce disj (into (sorted-set) xs) rm)]
      (doseq [[method set0] [["conj" (into (set/sorted-set) xs)]
                             ["bulk" (apply set/sorted-set xs)]
                             #?(:clj ["lazy" (storage/roundtrip (into (set/sorted-set) xs))])]
              :let [set1 (reduce disj set0 rm)
                    set2 (persistent! (reduce disj (transient set0) rm))
                    set3 (reduce disj set0 full-rm)
                    set4 (persistent! (reduce disj (transient set0) full-rm))]]
        (testing
          (str "Iter:" (inc i)  "/" iters
            "set:" method 
            "adds:" (str (count xs) " (" (count xs-sorted) " distinct),")
            "removals:" (str (count rm) " (down to " (count xs-rm) ")"))
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
            (is (= set4 #{}))))))))

(deftest stresstest-slice
  (println "  testing stresstest-slice...")
  (dotimes [i iters]
    (let [xs        (repeatedly (+ 1 (rand-int 20000)) #(rand-int 20000))
          xs-sorted (distinct (sort xs))
          [from to] (sort [(- 10000 (rand-int 20000)) (+ 10000 (rand-int 20000))])
          expected  (filter #(<= from % to) xs-sorted)]
      (doseq [[method set] [["conj" (into (set/sorted-set) xs)]
                            #?(:clj ["lazy" (storage/roundtrip (into (set/sorted-set) xs))])]
              :let [set-range (set/slice set from to)]]
        (testing
          (str
            "Iter: " (inc i) "/" iters
            ", set:" method
            ", from:" (count xs-sorted) " elements"
            ", down to:" (count expected))
          (let [set (into (set/sorted-set) (shuffle xs-sorted))]
            (is (= (set/rslice set 30000 -10)
                  (-> (set/rslice set 30000 -10) rseq reverse))))
          (is (= (vec set-range) (vec (seq set-range)))) ;; checking IReduce on BTSetIter
          (is (= (vec set-range) expected))
          (is (= (into-via-doseq [] set-range) expected))
          (is (= (vec (rseq set-range)) (reverse expected)))
          (is (= (vec (rseq (rseq set-range))) expected)))))))

(deftest stresstest-rslice
  (println "  testing stresstest-rslice...")
  (dotimes [i 1000]
    (let [len 3000
          xs  (vec (shuffle (range 0 (inc 3000))))
          s   (into (set/sorted-set) xs)]
      (testing (str "Iter: " i "/1000")
        (is (= 
              (set/rslice s (+ 3000 100) -100)
              (-> (set/rslice s (+ 3000 100) -100) rseq reverse)))))))

(deftest stresstest-seek
  (println "  testing stresstest-seek...")
  (dotimes [i iters]
    (let [xs        (repeatedly (inc (rand-int 20000)) #(rand-int 20000))
          xs-sorted (distinct (sort xs))
          seek-to   (rand-int 20000)
          set       (into (set/sorted-set) xs-sorted)]
      (testing (str "Iter: " i "/" iters ", seek to " seek-to)
        (is (= (seq (drop-while #(< % seek-to) xs-sorted))
              (set/seek (seq set) seek-to)))
        
        (is (= (seq (drop-while #(< % 19999) xs-sorted))
              (set/seek (seq set) 19999)))
        
        (is (= (seq (reverse (take-while #(<= % seek-to) xs-sorted)))
              (set/seek (rseq set) seek-to)))
        
        (is (= (seq (reverse (take-while #(<= % 1) xs-sorted)))
              (set/seek (rseq set) 1)))))))

(deftest test-overflow
  (println "  testing test-overflow...")
  (let [len  4000000
        part (quot len 100)
        xss  (partition-all part (shuffle (range 0 len)))
        s    (reduce into (set/sorted-set) xss)]
    (is (= 10 (count (take 10 s))))))
