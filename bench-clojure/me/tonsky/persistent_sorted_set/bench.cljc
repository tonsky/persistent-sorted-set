(ns me.tonsky.persistent-sorted-set.bench
  (:require
    #?(:clj [clj-async-profiler.core :as profiler])
    #?(:clj [criterium.core :as criterium])
    [me.tonsky.persistent-sorted-set :as set]
    [me.tonsky.persistent-sorted-set.bench.core :as bench.core]
    #?(:clj [me.tonsky.persistent-sorted-set.test.storage :as storage])))

(def ints-10K
  (vec (shuffle (range 10000))))

(def set-10K
  (into (set/sorted-set) ints-10K))

(def ints-50K
  (vec (shuffle (range 50000))))

(def set-50K
  (into (set/sorted-set) ints-50K))

(def ints-300K
  (vec (shuffle (range 300000))))

(def set-300K
  (into (set/sorted-set) ints-300K))

#?(:clj
    (def storage-300K
      (storage/storage)))

#?(:clj
    (def address-300K
      (set/store (into (set/sorted-set) ints-300K) storage-300K)))

(defn conj-10K []
  (reduce conj (set/sorted-set) ints-10K))

#?(:clj
    (defn conj-transient-10K []
      (persistent! (reduce conj! (transient (set/sorted-set)) ints-10K))))

(defn disj-10K []
  (reduce disj set-10K ints-10K))

#?(:clj
    (defn disj-transient-10K []
      (persistent! (reduce disj! (transient set-10K) ints-10K))))

(defn contains-10K []
  (doseq [x ints-10K]
    (contains? set-10K x)))

(defn doseq-300K []
  (let [*res (volatile! 0)]
    (doseq [x set-300K]
      (vswap! *res + x))
    @*res))

(defn next-300K []
  (loop [xs  set-300K
         res 0]
    (if-some [x (first xs)]
      (recur (next xs) (+ res x))
      res)))

(defn reduce-300K []
  (reduce + 0 set-300K))

#?(:clj
    (defn into-50K []
      (into (set/sorted-set) ints-50K)))

#?(:clj
    (defn store-50K []
      (set/store
        (into (set/sorted-set) ints-50K)
        (storage/storage))))

#?(:clj
    (defn reduce-300K-lazy []
      (reset! (:*memory storage-300K) {})
      (reduce + 0 (set/restore address-300K storage-300K))))

(def benches
  {"conj-10K"        conj-10K
   "disj-10K"        disj-10K
   "contains-10K"   contains-10K
   "doseq-300K"      doseq-300K
   "next-300K"       next-300K
   "reduce-300K"     reduce-300K
   #?@(:clj 
        ["conj-transient-10K" conj-transient-10K
         "disj-transient-10K" disj-transient-10K
         "into-50K"           into-50K
         "store-50K"          store-50K
         "reduce-300K-lazy"   reduce-300K-lazy])})

(defn ^:export -main [& args]
  (let [names    (or (not-empty args) (sort (keys benches)))
        _        (apply println #?(:clj "CLJ:" :cljs "CLJS:") names)
        longest  (last (sort-by count names))]
    (doseq [name names
            :let [fn (benches name)]]
      (if (nil? fn)
        (println "Unknown benchmark:" name)
        (let [{:keys [mean-ms]} (bench.core/bench (fn))]
          (println
            (bench.core/right-pad name (count longest))
            " "
            (bench.core/left-pad (bench.core/round mean-ms) 6) "ms/op"))))))
