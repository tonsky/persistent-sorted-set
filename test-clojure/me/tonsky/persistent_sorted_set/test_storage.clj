(ns me.tonsky.persistent-sorted-set.test-storage
  (:require
    [clojure.string :as str]
    [clojure.test :as t :refer [is are deftest testing]]
    [me.tonsky.persistent-sorted-set :as set])
  (:import
    [clojure.lang RT]
    [java.util Comparator Arrays]
    [me.tonsky.persistent_sorted_set ArrayUtil Edit IStorage Leaf Node PersistentSortedSet]))

(set! *warn-on-reflection* true)

(defn gen-addr []
  (str/join (repeatedly 10 #(rand-nth "ABCDEFGHIJKLMNOPQRSTUVWXYZ"))))

(defn leaf? [node]
  (not (instance? Node node)))

(defn persist
  ([^PersistentSortedSet set]
   (let [root     (.-_root set)
         *storage (atom (transient {}))
         address  (persist *storage root 0)]
     [address (persistent! @*storage)]))
  ([*storage ^Leaf leaf depth]
   (let [address (str depth "-" (gen-addr))
         count   (.count leaf nil)
         keys    (into [] (take (.-_len leaf)) (.keys leaf nil))]
     (swap! *storage assoc! address 
       (if (leaf? leaf)
         keys
         (let [node     ^Node leaf
               children (.children node nil)
               child-fn (if (leaf? (first children))
                          (fn [^Leaf leaf] (into [] (take (.-_len leaf)) (.keys leaf nil)))
                          #(persist *storage % (inc depth)))]
           {:keys     keys
            :count    count
            :children (into []
                        (comp
                          (take (.-_len node))
                          (map child-fn))
                        children)})))
     address)))

(defn print-storage
  ([address storage]
   (print-storage address storage ""))
  ([address storage indent]
   (let [node (storage address)]
     (println indent address (:count node))
     (doseq [child (:children node)]
       (if (vector? child)
         (println indent " [" (take 5 child) "... <" (count child) "elements>]")
         (print-storage child storage (str indent "  ")))))))

(defn wrap-storage [storage]
  (reify IStorage
    (^void load [_ ^Node node]
      (let [address (.-_address node)
            edit    (.-_edit node)
            data    (storage address)
            {:keys [keys children count]} data
            keys     (to-array keys)
            ; _        (println "  loading" address)
            children (if (vector? (first children))
                       (into-array Leaf (map #(Leaf. (to-array %) edit) children))
                       (into-array Leaf (map #(Node. % edit) children)))]
        (.onLoad node keys children count)
        nil))))

(defn lazy-load [original]
  (let [[address storage] (persist original)]
    (set/load RT/DEFAULT_COMPARATOR (wrap-storage storage) address)))

(deftest test-roundtrip
  (let [xs       (shuffle (range 1000000))
        rm       (vec (repeatedly (rand-int 500000) #(rand-nth xs)))
        original (reduce disj (into (set/sorted-set) xs) rm)
        [address storage] (persist original)
        loaded   (set/load RT/DEFAULT_COMPARATOR (wrap-storage storage) address)]
    ; (println "Added" (count xs) ", removed" (count rm) ", stayed" (count original))
    ; (println (count storage) (keys storage) address (storage address))
    ; (println (count loaded) (take 10 loaded) (take 10 (reverse loaded)) (set/slice loaded 5000 5100))
    ; (print-storage address storage)
    (println "count:")
    (is (= (count loaded) (count original)))
    (println "take 100:")
    (is (= (take 100 loaded) (take 100 original)))
    (println "take 5000:")
    (is (= (take 5000 loaded) (take 5000 original)))
    (println "slice 495000..505000")
    (is (= (vec (set/slice loaded 495000 505000)) (vec (set/slice loaded 495000 505000))))
    ; (println "slice 400000..500000")
    ; (is (= (vec (set/slice loaded 400000 500000)) (vec (set/slice loaded 400000 500000))))
    (println "take 100 reverse")
    (is (= (take 100 (rseq loaded)) (take 100 (rseq original))))
    (println "slice 990000 1000000")
    (is (= (vec (set/slice loaded 990000 1000000)) (vec (set/slice loaded 990000 1000000))))
    (println "conj -1")
    (is (= (conj loaded -1) (conj original -1)))
    (println "conj 100")
    (is (= (conj loaded 100) (conj original 100)))
    (println "conj 500000")
    (is (= (conj loaded 500000) (conj original 500000)))
    (println "conj Long/MAX_VALUE")
    (is (= (conj loaded Long/MAX_VALUE) (conj original Long/MAX_VALUE)))
    (println "disj take 100")
    (is (= (reduce disj loaded (take 100 loaded)) (reduce disj original (take 100 loaded))))
    ))
