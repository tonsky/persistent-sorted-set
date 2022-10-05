(ns me.tonsky.persistent-sorted-set.bench
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clj-async-profiler.core :as profiler]
    [criterium.core :as criterium]
    [me.tonsky.persistent-sorted-set :as set]))

(set/set-branching-factor! 64)

(when-not (.exists (io/file "bench-clojure/ints-10K.edn"))
  (let [xs (vec (shuffle (range 10000)))]
    (spit "bench-clojure/ints-10K.edn" (pr-str xs))))

(def ints-10K
  (edn/read-string (slurp "bench-clojure/ints-10K.edn")))

(def set-10K
  (into (set/sorted-set) ints-10K))

(when-not (.exists (io/file "bench-clojure/ints-300K.edn"))
  (let [xs (vec (shuffle (range 300000)))]
    (spit "bench-clojure/ints-300K.edn" (pr-str xs))))

(def ints-300K
  (edn/read-string (slurp "bench-clojure/ints-300K.edn")))

(def set-300K
  (into (set/sorted-set) ints-300K))

(defn bench-conj-10K []
  (reduce conj (set/sorted-set) ints-10K))

(defn bench-conj-transient-10K []
  (persistent! (reduce conj! (transient (set/sorted-set)) ints-10K)))

(defn bench-disj-10K []
  (reduce disj set-10K ints-10K))

(defn bench-disj-transient-10K []
  (persistent! (reduce disj! (transient set-10K) ints-10K)))

(defn bench-contains?-10K []
  (doseq [x ints-10K]
    (contains? set-10K x)))

(defn bench-contains-fn-10K []
  (doseq [x ints-10K]
    (set-10K x)))

(defn bench-iterate-300K []
  (let [*res (volatile! 0)]
    (doseq [x set-300K]
      (vswap! *res + x))
    @*res))

(defn bench-reduce-300K []
  (reduce + 0 set-300K))

(defn bench [sym]
  (let [fn            (resolve sym)
        _             (print (format "%-30s" (name sym)))
        _             (flush)
        results       (criterium/quick-benchmark (fn) {})
        [mean & _]    (:mean results)
        [factor unit] (criterium/scale-time mean)]
    (println (criterium/format-value mean factor unit))
    #_(profiler/profile
      (dotimes [i 10000]
        (fn)))))

(defn -main []
  (bench `bench-conj-10K)
  (bench `bench-conj-transient-10K)
  (bench `bench-disj-10K)
  (bench `bench-disj-transient-10K)
  (bench `bench-contains?-10K)
  (bench `bench-contains-fn-10K)
  (bench `bench-iterate-300K)
  (bench `bench-reduce-300K))
