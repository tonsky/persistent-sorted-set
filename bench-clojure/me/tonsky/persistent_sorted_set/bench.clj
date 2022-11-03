(ns me.tonsky.persistent-sorted-set.bench
  (:require
    [clojure.edn :as edn]
    [clojure.java.io :as io]
    [clj-async-profiler.core :as profiler]
    [criterium.core :as criterium]
    [me.tonsky.persistent-sorted-set :as set]
    [me.tonsky.persistent-sorted-set.test-storage :as test-storage]))

(set/set-branching-factor! 64)

(defn print-gc []
  (let [beans (java.lang.management.ManagementFactory/getGarbageCollectorMXBeans)
        count (->> beans (map #(.getCollectionCount %)) (filter pos?) (reduce + 0))
        time  (->> beans (map #(.getCollectionTime %)) (filter pos?) (reduce + 0))]
    (println (format "Count %d, time %d" count time))))

(when-not (.exists (io/file "bench-clojure/ints-10K.edn"))
  (let [xs (vec (shuffle (range 10000)))]
    (spit "bench-clojure/ints-10K.edn" (pr-str xs))))

(def ints-10K
  (edn/read-string (slurp "bench-clojure/ints-10K.edn")))

(def set-10K
  (into (set/sorted-set) ints-10K))

(when-not (.exists (io/file "bench-clojure/ints-50K.edn"))
  (let [xs (vec (shuffle (range 50000)))]
    (spit "bench-clojure/ints-50K.edn" (pr-str xs))))

(def ints-50K
  (edn/read-string (slurp "bench-clojure/ints-50K.edn")))

(def set-50K
  (into (set/sorted-set) ints-50K))

(when-not (.exists (io/file "bench-clojure/ints-300K.edn"))
  (let [xs (vec (shuffle (range 300000)))]
    (spit "bench-clojure/ints-300K.edn" (pr-str xs))))

(def ints-300K
  (edn/read-string (slurp "bench-clojure/ints-300K.edn")))

(def set-300K
  (into (set/sorted-set) ints-300K))

(def storage-300K
  (test-storage/storage))

(def address-300K
  (set/store (into (set/sorted-set) ints-300K) storage-300K))

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

(defn bench-into-50K []
  (into (set/sorted-set) ints-50K))

(defn bench-store-50K []
  (set/store
    (into (set/sorted-set) ints-50K)
    (test-storage/storage)))

(defn bench-reduce-300K []
  (reduce + 0 set-300K))

(defn bench-reduce-300K-lazy []
  (reset! (:*memory storage-300K) {})
  (reduce + 0 (set/restore address-300K storage-300K)))

(defn bench [sym]
  (let [fn            (resolve sym)
        _             (print (format "%-30s" (name sym)))
        _             (flush)
        results       (criterium/quick-benchmark (fn) {})
        [mean & _]    (:mean results)
        [factor unit] (criterium/scale-time mean)]
    (println (criterium/format-value mean factor unit))))

(defn -main []
  (bench `bench-conj-10K)
  (bench `bench-conj-transient-10K)
  (bench `bench-disj-10K)
  (bench `bench-disj-transient-10K)
  (bench `bench-contains?-10K)
  (bench `bench-contains-fn-10K)
  (bench `bench-iterate-300K)
  (bench `bench-reduce-300K)
  (bench `bench-into-50K)  
  (bench `bench-store-50K)
  (bench `bench-reduce-300K)
  (bench `bench-reduce-300K-lazy))
  
