(ns user
  (:require
    [clojure.string :as str]
    [io.github.humbleui.app :as app]
    [io.github.humbleui.canvas :as canvas]
    [io.github.humbleui.core :as core]
    [io.github.humbleui.debug :as debug]
    [io.github.humbleui.paint :as paint]
    [io.github.humbleui.window :as window]
    [io.github.humbleui.ui :as ui]
    [me.tonsky.persistent-sorted-set :as set]
    [me.tonsky.persistent-sorted-set.test-storage :as test-storage]
    [nrepl.cmdline :as nrepl])
  (:import
    [clojure.lang RT]
    [io.github.humbleui.skija Canvas]
    [io.github.humbleui.types IRect]
    [me.tonsky.persistent_sorted_set Leaf Node PersistentSortedSet]))

(set! *warn-on-reflection* true)

(defonce *window (atom nil))

(defn ^PersistentSortedSet make-db [size]
  (let [xs       (shuffle (range size))
        original (into (set/sorted-set) xs)
        [address storage] (test-storage/persist original)]
    (set/load RT/DEFAULT_COMPARATOR (test-storage/wrap-storage storage) address)))

(defonce db
  (make-db 1000000))

(defn leaf? [^Leaf node]
  (not (instance? Node node)))

(defn loaded? [^Leaf node]
  (if (leaf? node)
    true
    (pos? (.-_count ^Node node))))

(defn stats [^PersistentSortedSet set]
  (let [root ^Node (.-_root set)
        l1   (for [^Node l1 (.-_children root)
                   :when (loaded? l1)]
               l1)
        l2   (for [^Node l1 (.-_children root)
                   :when (loaded? l1)
                   ^Node l2 (.-_children l1)
                   :when (loaded? l2)]
               l2)]
    {:l0-count   (.-_count root)
     :l1-nodes   (count l1)
     :l1-len     (.-_len root)
     :l2-count   (reduce + (map #(.-_count ^Node %) l2))
     :l2-nodes   (count l2)
     :l2-len     (reduce + 0
                   (for [^Node l1 (.-_children root)]
                     (if (loaded? l1)
                       (.-_len l1)
                       32)))}))

(def fill-leaf   (paint/fill 0x4033CC33))
(def fill-loaded (paint/fill 0xFF33CC33))
(def fill-lazy   (paint/fill 0x40CC3333))

(defn node [^Leaf node]
  (ui/width 8
    (ui/height 8
      (ui/canvas
        {:on-paint
         (fn [ctx canvas size]
           (let [scale (:scale ctx)]
             (if (leaf? node)
               (let [cnt   (.-_len node)
                     h1    (quot cnt 8)
                     w2    (mod cnt 8)]
                 (canvas/draw-rect canvas (IRect/makeXYWH 0 0 (* 8 scale) (* 8 scale)) fill-leaf)
                 (canvas/draw-rect canvas (IRect/makeXYWH 0 0 (* 8 scale) (* h1 scale)) fill-loaded)
                 (when (not= w2 0)
                   (canvas/draw-rect canvas (IRect/makeXYWH 0 (* h1 scale) (* w2 scale) (* 1 scale)) fill-loaded)))
               
               (canvas/draw-rect canvas (IRect/makeXYWH 0 0 (* 8 scale) (* 8 scale)) (if (loaded? node) fill-loaded fill-lazy)))))}))))

(defn draw-node [^Canvas canvas ^Leaf node depth size]
  (cond
    (leaf? node)
    (canvas/draw-rect canvas (IRect/makeXYWH (- (/ size 2)) (- (/ size 2)) size size) fill-loaded)
    
    (loaded? node)
    (canvas/with-canvas canvas
      (canvas/draw-rect canvas (IRect/makeXYWH (- (/ size 2)) (- (/ size 2)) size size) fill-loaded)
      (dotimes [i (.-_len node)]
        (let [offset (* size 7)]
          (canvas/draw-rect canvas (IRect/makeXYWH 0 (- offset) 1 offset) fill-leaf)
          (canvas/translate canvas 0 offset)
          (draw-node canvas (aget (.-_children ^Node node) i) (inc depth) (/ size 2.5))
          (canvas/translate canvas 0 (- offset))
          (canvas/rotate canvas (/ 360 (.-_len node))))))
        
    :else
    (canvas/draw-rect canvas (IRect/makeXYWH (- (/ size 2)) (- (/ size 2)) size size) fill-lazy)))
  

(def app
  (ui/canvas
    {:on-paint
     (fn [ctx ^Canvas canvas size]
       (canvas/with-canvas canvas
         (canvas/translate canvas (/ (:width size) 2) (/ (:height size) 2))
         (draw-node canvas (.-_root ^PersistentSortedSet db) 0 64)))}))
       

#_(def app
  (ui/default-theme
    (ui/valign 0.5
      (ui/vscrollbar
        (ui/vscroll
          (ui/padding 8 8
            (ui/dynamic _ [_ (rand)]
              (ui/row
                [:stretch 1
                (ui/halign 0.5
                  (let [l0 (.-_root db)]
                    (ui/column
                      (ui/halign 0.5 (node l0))
                      (ui/gap 0 8)
                      (when (loaded? l0)
                        (ui/row
                          (interpose
                            (ui/gap 8 0)
                            (for [l1 (.-_children ^Node l0)]
                              (ui/column
                                (node l1)
                                (ui/gap 0 8)
                                (when (loaded? l1)
                                  (interpose
                                    (ui/gap 0 1)
                                    (for [l2 (.-_children ^Node l1)]
                                      (node l2))))))))))))]
                (ui/width #(* 0.3 (:width %))
                 (ui/padding 16 16
                   (let [stats (stats db)]
                     (ui/row
                       (ui/column
                         (ui/label "")
                         (ui/gap 0 16)
                         (ui/label "L1 nodes")
                         (ui/gap 0 16)
                         (ui/label "L2 nodes")
                         (ui/gap 0 16)
                         (ui/label "Items"))
                       (ui/gap 8 0)
                       (ui/column
                         (ui/halign 1 (ui/label "Loaded"))
                         (ui/gap 0 16)
                         (ui/halign 1 (ui/label (format "%,d"    (:l1-nodes stats))))
                         (ui/gap 0 16)
                         (ui/halign 1 (ui/label (format "%,d"    (:l2-nodes stats))))
                         (ui/gap 0 16)
                         (ui/halign 1 (ui/label (format "%,d"    (:l2-count stats)))))
                       (ui/gap 8 0)
                       (ui/column
                         (ui/halign 1 (ui/label "Total"))
                         (ui/gap 0 16)
                         (ui/halign 1 (ui/label (format "%,d"    (:l1-len stats))))
                         (ui/gap 0 16)
                         (ui/halign 1 (ui/label (format "%,d"    (:l2-len stats))))
                         (ui/gap 0 16)
                         (ui/halign 1 (ui/label (format "%,d"    (:l0-count stats)))))
                       (ui/gap 8 0)
                       (ui/column
                         (ui/halign 1 (ui/label "Percent"))
                         (ui/gap 0 16)
                         (ui/halign 1 (ui/label (format "%.1f%%" (double (* 100 (/ (:l1-nodes stats) (:l1-len stats)))))))
                         (ui/gap 0 16)
                         (ui/halign 1 (ui/label (format "%.1f%%" (double (* 100 (/ (:l2-nodes stats) (:l2-len stats)))))))
                         (ui/gap 0 16)
                         (ui/halign 1 (ui/label (format "%.1f%%" (double (* 100 (/ (:l2-count stats) (:l0-count stats))))))))))))))))))))

(defn ctx [window]
  (when-not (window/closed? window)
    {:window window
     :scale  (window/scale window)}))

(defn on-paint [window canvas]
  (canvas/clear canvas 0xFFF6F6F6)
  (let [bounds (window/content-rect window)]
    (core/draw app (ctx window) (IRect/makeXYWH 0 0 (:width bounds) (:height bounds)) canvas)))

(defn redraw []
  (some-> @*window window/request-frame))

(redraw)

(defn on-event [window event]
  (when-let [result (core/event app (ctx window) event)]
    (window/request-frame window)
    result))
   
(defn make-window []
  (let [screen  (first (app/screens))
        scale   (:scale screen)
        area    (:work-area screen)
        width   (- (:width area) (* scale 100))
        height  (- (:height area) (* scale 100))
        x       (+ (:x area) (* scale 50))
        y       (+ (:y area) (* scale 50))
        window  (window/make
                  {:on-close #(reset! *window nil)
                   :on-paint #'on-paint
                   :on-event #'on-event})]
    (reset! debug/*enabled? true)
    (window/set-title window "Humble UI 🐝")
    (window/set-window-size window width height)
    (window/set-window-position window x y)
    (window/set-visible window true)))

(defn -main [& args]
  (future (apply nrepl/-main args))
  (app/start #(reset! *window (make-window))))

(comment
  (do
    (app/doui (some-> @*window window/close))
    (reset! *window (app/doui (make-window))))
  (first db)
  (set/slice db 5000 6000)
  (set/slice db 500000 1000000)
  (take 10 (rseq db))
  (conj db 1)
  (def db (make-db 10000))
  (def db (make-db 100000))
  (def db (make-db 400000))
  (def db (make-db 1000000))
  (def db (make-db 2000000))
  (def db (make-db 4000000))

  (stats db)
  (redraw)
  (do
    #_(Thread/sleep 1000)
    (dotimes [i 500]
      (contains? db (rand-int (count db)))
      (redraw)
      (Thread/sleep 8)))
  )