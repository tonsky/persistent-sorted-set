(ns me.tonsky.persistent-sorted-set.test.transient-corruption
  "Regression tests for issue-19: Branch.add() on the non-editable path used to
   share _keys with the node it created. During transient ops the new node is
   editable, so subsequent in-place edits mutated the persistent base set’s
   arrays, corrupting its separator keys"
  (:require
    [me.tonsky.persistent-sorted-set :as set]
    [clojure.test :as t :refer [is deftest testing]]))

(set! *warn-on-reflection* true)

(defn- run-ops! [t ops]
  (reduce
    (fn [t [op v]]
      (case op
        :add    (conj! t v)
        :remove (disj! t v)))
    t ops))

(defn- base-intact? [base elems ops]
  (persistent! (run-ops! (transient base) ops))
  (and
    (t/is (= elems (vec base)) (str "seq broken after " ops))
    (t/is (every? #(contains? base %) elems) (str "contains broken after " ops))
    (t/is (= #{} (reduce disj base elems)) (str "disj broken after " ops))))

(deftest test-transient-does-not-corrupt-base
  (testing "minimal repro from issue-19"
    ;; conj! -1 copies the first leaf but shares root’s _keys,
    ;; disj! 2 then rewrites the shared separator key in place,
    ;; sending binary search for 2 into the wrong child
    (let [base (into (set/sorted-set* {:branching-factor 4}) (range 0 14 2))]
      (persistent! (-> (transient base) (conj! -1) (disj! 2)))
      (is (= (range 0 14 2) (seq base)))
      (is (contains? base 2))))

  (testing "exhaustive small cases"
    (doseq [n (range 3 17)
            :let [elems (vec (range 0 (* 2 n) 2))
                  base  (into (set/sorted-set* {:branching-factor 4}) elems)]
            x (range -1 (* 2 n) 2)
            y elems]
      (base-intact? base elems [[:add x] [:remove y]])))

  (testing "default branching factor"
    (let [elems (vec (range 0 8192 2))
          base  (into (set/sorted-set) elems)
          ops   (concat
                  (for [x (range 1 2048 2)] [:add x])
                  (for [y (range 0 2048 2)] [:remove y])
                  (for [x (range 4096 6144)] [:add x]))]
      (base-intact? base elems ops))))
