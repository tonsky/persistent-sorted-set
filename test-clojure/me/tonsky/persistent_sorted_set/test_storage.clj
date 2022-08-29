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
  (random-uuid)
  #_(str/join (repeatedly 10 #(rand-nth "ABCDEFGHIJKLMNOPQRSTUVWXYZ"))))

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

(deftest test-lazyness
  ; (PersistentSortedSet/setMaxLen 1024)
  (let [xs       (shuffle (range 1000000))
        rm       (vec (repeatedly (rand-int 500000) #(rand-nth xs)))
        original (-> (reduce disj (into (set/sorted-set) xs) rm)
                   (disj 250000 500000))
        [address storage] (persist original)
        loaded   (set/load RT/DEFAULT_COMPARATOR (wrap-storage storage) address)
        
        ; count shouldn’t fetch anything but root
        _       (is (= (count loaded) (count original)))
        l0      (:loaded-ratio (set/stats loaded))
        _       (is (= 0.0 l0))
        
        ; touch first 100
        _       (is (= (take 100 loaded) (take 100 original)))
        l100    (:loaded-ratio (set/stats loaded))
        _       (is (< 0 l100 1.0))
    
        ; touch first 5000
        _       (is (= (take 5000 loaded) (take 5000 original)))
        l5000   (:loaded-ratio (set/stats loaded))
        _       (is (< l100 l5000 1.0))
    
        ; touch middle
        _       (is (= (vec (set/slice loaded 495000 505000)) (vec (set/slice loaded 495000 505000))))
        lmiddle (:loaded-ratio (set/stats loaded))
        _       (is (< l5000 lmiddle 1.0))
        
        ; touch 100 last
        _       (is (= (take 100 (rseq loaded)) (take 100 (rseq original))))
        lrseq   (:loaded-ratio (set/stats loaded))
        _       (is (< lmiddle lrseq 1.0))
    
        ; touch 10000 last
        _       (is (= (vec (set/slice loaded 990000 1000000)) (vec (set/slice loaded 990000 1000000))))
        ltail   (:loaded-ratio (set/stats loaded))
        _       (is (< lrseq ltail 1.0))
    
        ; conj to beginning
        loaded' (conj loaded -1)
        _       (is (= ltail (:loaded-ratio (set/stats loaded'))))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
        
        ; conj to middle
        loaded' (conj loaded 500000)
        _       (is (= ltail (:loaded-ratio (set/stats loaded'))))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
        
        ; conj to end
        loaded' (conj loaded Long/MAX_VALUE)
        _       (is (= ltail (:loaded-ratio (set/stats loaded'))))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
        
        ; conj to untouched area
        loaded' (conj loaded 250000)
        _       (is (< ltail (:loaded-ratio (set/stats loaded')) 1.0))
        _       (is (< ltail (:loaded-ratio (set/stats loaded)) 1.0))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
    
        ; transients conj
        xs      (range 10000)
        loaded' (into loaded xs)
        _       (is (every? loaded' xs))
        _       (is (< ltail (:loaded-ratio (set/stats loaded'))))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
    
        ; transient disj
        lratio  (:loaded-ratio (set/stats loaded))
        xs      (take 100 loaded)
        loaded' (reduce disj loaded xs)
        _       (is (every? #(not (loaded' %)) xs))
        _       (is (= lratio (:loaded-ratio (set/stats loaded'))))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
        ]))
