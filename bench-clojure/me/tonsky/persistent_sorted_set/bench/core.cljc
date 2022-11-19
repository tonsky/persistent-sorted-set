(ns me.tonsky.persistent-sorted-set.bench.core
  #?(:cljs
      (:require-macros me.tonsky.persistent-sorted-set.bench.core)))

; Measure time

(def ^:dynamic *warmup-ms* 2000)
(def ^:dynamic *bench-ms*  1000)
(def ^:dynamic *samples*   5)
(def ^:dynamic *batch*     10)

#?(:cljs (defn ^number now [] (js/performance.now))
    :clj  (defn now ^double [] (/ (System/nanoTime) 1000000.0)))

#?(:clj
    (defmacro dotime
      "Runs form duration, returns average time (ms) per iteration"
      [duration & body]
      `(let [start-t# (now)
             end-t#   (+ ~duration start-t#)]
         (loop [iterations# *batch*]
           (dotimes [_# *batch*] ~@body)
           (let [now# (now)]
             (if (< now# end-t#)
               (recur (+ *batch* iterations#))
               (double (/ (- now# start-t#) iterations#))))))))

(defn- if-cljs [env then else]
  (if (:ns env) then else))

(defn median [xs]
  (nth (sort xs) (quot (count xs) 2)))

(defn to-fixed [n places]
  #?(:cljs (.toFixed n places)
      :clj  (String/format java.util.Locale/ROOT (str "%." places "f") (to-array [(double n)]))))

(defn round [n]
  (cond
    (> n 1)    (to-fixed n 1)
    (> n 0.01) (to-fixed n 3)
    :else      (to-fixed n 7)))

(defn left-pad [s l]
  (if (<= (count s) l)
    (str (apply str (repeat (- l (count s)) " ")) s)
    s))

(defn right-pad [s l]
  (if (<= (count s) l)
    (str s (apply str (repeat (- l (count s)) " ")))
    s))

#?(:clj
    (defmacro bench
      "Runs for *wramup-ms* + *bench-ms*, returns median time (ms) per iteration"
      [& body]
      (if-cljs &env
        `(let [_#     (dotime *warmup-ms* ~@body)
               times# (mapv
                        (fn [_#]
                          (dotime *bench-ms* ~@body))
                        (range *samples*))]
           {:mean-ms (median times#)})
        `(let [results#     (criterium.core/quick-benchmark (do ~@body) {})
               [mean# & _#] (:mean results#)]
           {:mean-ms (* mean# 1000.0)}))))