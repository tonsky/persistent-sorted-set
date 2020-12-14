(defproject persistent-sorted-set "0.1.2"
  :description "Fast B-tree based persistent sorted set for Clojure/Script"
  :license {:name "MIT"}
  :url "https://github.com/tonsky/persistent-sorted-set"

  :dependencies [
    [org.clojure/clojure       "1.10.1"   :scope "provided"]
    [org.clojure/clojurescript "1.10.773" :scope "provided"]
  ]

  :plugins [
    [lein-cljsbuild "1.1.7"]
    ; [lein-virgil "0.1.9"]
  ]

  :source-paths      ["src-clojure"]
  :java-source-paths ["src-java"]
  :test-paths        ["test-clojure"]

  :javac-options ["-target" "8" "-source" "8" "-bootclasspath" ~(str (or (System/getenv "JAVA8_HOME") (throw (Exception. "Please set JAVA8_HOME"))) "/jre/lib/rt.jar")]
  :jvm-opts ["-ea"]

  :aliases {"test-all"  ["do" ["test"] ["test-cljs"]]
            "test-cljs" ["run" "-m" "me.tonsky.persistent-sorted-set.repl/run-tests"]
            "repl-cljs" ["run" "-m" "me.tonsky.persistent-sorted-set.repl/repl"]
            "bench"     ["trampoline" "with-profile" "+bench" "run" "-m" "me.tonsky.persistent_sorted_set.Bench"]}

  :profiles {
    :1.9 {:dependencies [[org.clojure/clojure         "1.9.0"   :scope "provided"]
                         [org.clojure/clojurescript   "1.9.946" :scope "provided"]] }
    :bench {:dependencies [[com.datomic/datomic-free "0.9.5703"]]
            :java-source-paths ["bench-java"] }
  }
)
