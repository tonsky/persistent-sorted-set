(ns me.tonsky.persistent-sorted-set.repl
  (:require
    [cljs.repl :as repl]
    [cljs.test :as test]
    [cljs.repl.node :as node]))


(defn run-tests []
  (repl/repl* (node/repl-env)
    {:output-dir     "target"
     :optimizations  :none
     :cache-analysis true
     :source-map     false
     :read           (fn [a b] b) ; exit immediately
     :inits [{:type :eval-forms
              :forms ['(enable-console-print!)
                      '(require 'me.tonsky.persistent-sorted-set.test)
                      '(require 'cljs.test)
                      '(cljs.test/run-tests 'me.tonsky.persistent-sorted-set.test)]}]}))


(defn repl []
  (repl/repl* (node/repl-env)
    {:output-dir     "target"
     :optimizations  :none
     :cache-analysis true
     :source-map     false
     :inits [{:type :eval-forms
              :forms ['(enable-console-print!)
                      '(require '[me.tonsky.persistent-sorted-set :as set])]}]}))
