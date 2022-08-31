(ns me.tonsky.persistent-sorted-set.test-storage
  (:require
    [clojure.string :as str]
    [clojure.test :as t :refer [is are deftest testing]]
    [me.tonsky.persistent-sorted-set :as set])
  (:import
    [clojure.lang RT]
    [java.util Comparator Arrays]
    [me.tonsky.persistent_sorted_set ArrayUtil IStorage Node PersistentSortedSet]))

(set! *warn-on-reflection* true)

(defn gen-addr []
  (random-uuid)
  #_(str/join (repeatedly 10 #(rand-nth "ABCDEFGHIJKLMNOPQRSTUVWXYZ"))))

(defn persist
  ([^PersistentSortedSet set]
   (persist {} set))
  ([storage ^PersistentSortedSet set]
   (let [root     (.-_root set)
         *storage (atom storage)
         *stats   (atom {:writes 0})
         address  (persist *storage *stats root 0)]
     {:address address
      :storage @*storage
      :stats   @*stats}))
  ([*storage *stats ^Node node depth]
   (if (.durable node)
     (.address node)
     (let [_       (assert (.loaded node))
           address (str depth "-" (gen-addr))
           len     (.len node nil)
           keys    (into [] (take len (.keys node nil)))]
       ; (println "Writing" address)
       (swap! *storage assoc address 
         (if (.leaf node nil)
           keys
           {:keys     keys
            :children (->> (.children node nil)
                        (take len)
                        (mapv #(persist *storage *stats % (inc depth))))}))
       (.onPersist node address)
       (swap! *stats update :writes inc)
       address))))

(defn wrap-storage [storage]
  (reify IStorage
    (^void load [_ ^Node node]
      (let [address (.-_address node)
            ; _       (println "  loading" address)
            data    (storage address)]
        (if (vector? data)
          (let [keys (to-array data)]
            (.onLoadLeaf node keys))
          (let [{:keys [keys children]} data
                keys     (to-array keys)
                children (into-array Node (map (fn [addr] (Node. addr)) children))]
            (.onLoadBranch node keys children)))))))

(defn lazy-load [original]
  (let [{:keys [address storage]} (persist original)]
    (set/load RT/DEFAULT_COMPARATOR (wrap-storage storage) address)))

(deftest test-lazyness
  (let [size     1000000
        xs       (shuffle (range size))
        rm       (vec (repeatedly (quot size 5) #(rand-nth xs)))
        original (-> (reduce disj (into (set/sorted-set) xs) rm)
                   (disj (quot size 4) (quot size 2)))
        {:keys [address storage stats]} (persist original)
        _       (is (> (:writes stats) (/ size PersistentSortedSet/MAX_LEN)))
        loaded  (set/load RT/DEFAULT_COMPARATOR (wrap-storage storage) address)
        _       (is (= 0.0 (:loaded-ratio (set/stats loaded))))
        _       (is (= 1.0 (:durable-ratio (set/stats loaded))))
                
        ; touch first 100
        _       (is (= (take 100 loaded) (take 100 original)))
        l100    (:loaded-ratio (set/stats loaded))
        _       (is (< 0 l100 1.0))
    
        ; touch first 5000
        _       (is (= (take 5000 loaded) (take 5000 original)))
        l5000   (:loaded-ratio (set/stats loaded))
        _       (is (< l100 l5000 1.0))
    
        ; touch middle
        from    (- (quot size 2) (quot size 200))
        to      (+ (quot size 2) (quot size 200))
        _       (is (= (vec (set/slice loaded from to))
                      (vec (set/slice loaded from to))))
        lmiddle (:loaded-ratio (set/stats loaded))
        _       (is (< l5000 lmiddle 1.0))
        
        ; touch 100 last
        _       (is (= (take 100 (rseq loaded)) (take 100 (rseq original))))
        lrseq   (:loaded-ratio (set/stats loaded))
        _       (is (< lmiddle lrseq 1.0))
    
        ; touch 10000 last
        from    (- size (quot size 100))
        to      size
        _       (is (= (vec (set/slice loaded from to))
                      (vec (set/slice loaded from 1000000))))
        ltail   (:loaded-ratio (set/stats loaded))
        _       (is (< lrseq ltail 1.0))
    
        ; conj to beginning
        loaded' (conj loaded -1)
        _       (is (= ltail (:loaded-ratio (set/stats loaded'))))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
        
        ; conj to middle
        loaded' (conj loaded (quot size 2))
        _       (is (= ltail (:loaded-ratio (set/stats loaded'))))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
        
        ; conj to end
        loaded' (conj loaded Long/MAX_VALUE)
        _       (is (= ltail (:loaded-ratio (set/stats loaded'))))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
        
        ; conj to untouched area
        loaded' (conj loaded (quot size 4))
        _       (is (< ltail (:loaded-ratio (set/stats loaded')) 1.0))
        _       (is (< ltail (:loaded-ratio (set/stats loaded)) 1.0))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
    
        ; transients conj
        xs      (range -10000 0)
        loaded' (into loaded xs)
        _       (is (every? loaded' xs))
        lprep   (:loaded-ratio (set/stats loaded'))
        _       (is (< ltail lprep))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
        
        ; incremental persist
        {stats' :stats} (persist storage loaded')
        _       (is (< (:writes stats') 350)) ;; ~ 10000 / 32 + 10000 / 32 / 32 + 1
        _       (is (= lprep (:loaded-ratio (set/stats loaded'))))
        _       (is (= 1.0 (:durable-ratio (set/stats loaded'))))
    
        ; transient disj
        xs      (take 100 loaded)
        loaded' (reduce disj loaded xs)
        _       (is (every? #(not (loaded' %)) xs))
        _       (is (< (:durable-ratio (set/stats loaded')) 1.0))
        
        ; count fetches everything
        _       (is (= (count loaded) (count original)))
        l0      (:loaded-ratio (set/stats loaded))
        _       (is (= 1.0 l0))
        ]))
