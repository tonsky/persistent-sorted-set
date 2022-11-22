(ns me.tonsky.persistent-sorted-set.test-small
  (:require
    [me.tonsky.persistent-sorted-set :as set]
    #?(:clj [me.tonsky.persistent-sorted-set.test-storage :as test-storage])
    #?(:cljs [cljs.test    :as t :refer-macros [is are deftest testing]]
        :clj  [clojure.test :as t :refer        [is are deftest testing]]))
  #?(:clj
      (:import [clojure.lang IReduce])))

#?(:clj (set! *warn-on-reflection* true))

(deftest test-small
  (is (= (range 10 20)
        (seq (into (set/sorted-set) (range 10 20)))))
  
  (is (= (range 10 20)
        (seq (into (set/sorted-set) (reverse (range 10 20))))))
  
  (is (= (range 10 20)
        (seq
          (set/slice
            (into (set/sorted-set) (reverse (range 0 100)))
            10
            19))))
  
  (is (= (range 19 9 -1)
        (rseq (into (set/sorted-set) (range 10 20)))))
  
  (is (= (range 19 9 -1)
        (set/rslice (into (set/sorted-set) (range 0 40)) 19 10)))
  
  (is (= (range 10 20)
        (set/slice (into (set/sorted-set) (range 0 40)) 10 19)))
  
  
  #_(loop [xs (range 29 -1 -1)
         s (set/sorted-set)]
    (println "\n" (seq s) (set/slice s 10 19))
    (if-some [x (first xs)]
      (recur (next xs) (conj s x))
      s)))