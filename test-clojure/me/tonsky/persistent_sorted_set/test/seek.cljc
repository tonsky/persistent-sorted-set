(ns me.tonsky.persistent-sorted-set.test.seek
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

(deftest stresstest-seek
  (println "[ TEST ] stresstest-seek")
  (dotimes [i iters]
    (let [xs        (repeatedly (inc (rand-int 20000)) #(rand-int 20000))
          xs-sorted (distinct (sort xs))
          seek-to (rand-int 20000)
          expected-seq  (filter #(<= seek-to %) xs-sorted)
          expected-rseq  (reverse (filter #(<= % seek-to) xs-sorted))
          set       (into (set/sorted-set) xs)]
      (testing (str "seek to " seek-to)
        (is (= expected-seq (seq (set/seek (seq set) seek-to))))
        (is (= expected-rseq (seq (set/seek (rseq set) seek-to)))))))
  (println "[ DONE ] stresstest-seek"))
