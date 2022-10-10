(ns me.tonsky.persistent-sorted-set.test-storage
  (:require
    [clojure.string :as str]
    [clojure.test :as t :refer [is are deftest testing]]
    [me.tonsky.persistent-sorted-set :as set])
  (:import
    [clojure.lang RT]
    [java.util Comparator Arrays]
    [me.tonsky.persistent_sorted_set ANode ArrayUtil Branch IStorage Leaf PersistentSortedSet]))

(set! *warn-on-reflection* true)

(defn gen-addr [depth]
  #_(random-uuid)
  (str depth "-" (str/join (repeatedly 10 #(rand-nth "ABCDEFGHIJKLMNOPQRSTUVWXYZ")))))

(defn persist
  ([^PersistentSortedSet set]
   (persist {} set))
  ([storage ^PersistentSortedSet set]
   (let [*storage (atom storage)
         *stats   (atom {:writes 0})
         address  (or (.-_address set)
                    (.address set
                      (persist *storage *stats (.-_root set) 0)))]
     {:address address
      :storage @*storage
      :stats   @*stats}))
  ([*storage *stats ^ANode node depth]
   (let [address (gen-addr depth)
         len     (.len node)
         keys    (->> (.-_keys node) (take len) (into []))]
     (swap! *storage assoc address 
       (if (instance? Leaf node)
         {:keys keys}
         {:keys keys
          :addresses
          (mapv
            (fn [idx child-address child]
              (or child-address
                (.address ^Branch node idx
                  (persist *storage *stats child (inc depth)))))
            (range len)
            (.-_addresses ^Branch node)
            (.-_children ^Branch node))}))
     (swap! *stats update :writes inc)
     address)))

(defn wrap-storage [storage]
  (reify IStorage
    (^ANode load [_ address]
      (let [{:keys [keys addresses]} (storage address)
            len (count keys)]
        (if addresses
          (Branch. len (to-array keys) (to-array addresses) nil nil)
          (Leaf. len (to-array keys) nil))))))

(defn lazy-load [original]
  (let [{:keys [address storage]} (persist original)]
    (set/load RT/DEFAULT_COMPARATOR (wrap-storage storage) address)))

(deftest test-lazy-remove
  "Check that invalidating middle branch does not invalidates siblings"
  (let [size 7000 ;; 3-4 branches
        xs   (shuffle (range size))
        set  (into (set/sorted-set) xs)]
    (persist set)
    (is (= 1.0 (:durable-ratio (set/stats set)))
      (let [set' (disj set 3500)] ;; one of the middle branches
        (is (< 0.99 (:durable-ratio (set/stats set'))))))))

(defmacro dobatches [[sym coll] & body]
  `(loop [coll# ~coll]
     (when (seq coll#)
       (let [batch# (rand-nth [1 2 3 4 5 10 20 30 40 50 100])
             [~sym tail#] (split-at batch# coll#)]
         ~@body
         (recur tail#)))))

(deftest stresstest-stable-addresses
  (let [size      10000
        adds      (shuffle (range size))
        removes   (shuffle adds)
        *set      (atom (set/sorted-set))
        *storage  (atom {})
        invariant (fn invariant 
                    ([o]
                     (invariant (.-_root ^PersistentSortedSet o) (some? (.-_address ^PersistentSortedSet o))))
                    ([o stored?]
                     (condp instance? o
                       Branch
                       (let [node ^Branch o
                             len (.len node)]
                         (doseq [i (range len)
                                 :let [addr   (nth (.-_addresses node) i)
                                       child  (nth (.-_children node) i)
                                       {:keys [keys addresses]} (get @*storage addr)]]
                           ;; nodes inside stored? has to ALL be stored
                           (when stored?
                             (is (some? addr)))
                           (when (some? addr)
                             (is (= keys 
                                   (take (.len ^ANode child) (.-_keys ^ANode child))))
                             (is (= addresses
                                   (when (instance? Branch child)
                                     (take (.len ^Branch child) (.-_addresses ^Branch child))))))
                           (invariant child (some? addr))))
                        
                       Leaf
                       true)))]
    
    (testing "Persist after each"
      (dobatches [xs adds]
        (let [set' (swap! *set into xs)]
          (invariant set')
          (reset! *storage (:storage (persist @*storage set')))))
      
      (invariant @*set)
      
      (dobatches [xs removes]
        (let [set' (swap! *set #(reduce disj % xs))]
          (invariant set')
          (reset! *storage (:storage (persist @*storage set'))))))
    
    (testing "Persist once"
      (reset! *set (into (set/sorted-set) adds))
      (reset! *storage (:storage (persist @*set)))
      (dobatches [xs removes]
        (let [set' (swap! *set #(reduce disj % xs))]
          (invariant set'))))))
    
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
        _       (is (= 1.0 l0))]))
