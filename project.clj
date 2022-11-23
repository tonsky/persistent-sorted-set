(defproject persistent-sorted-set "0.0.0"
  :description "Fast B-tree based persistent sorted set for Clojure/Script"
  :license {:name "MIT"}
  :url "https://github.com/tonsky/persistent-sorted-set"
  
  :dependencies
  [[org.clojure/clojure       "1.11.1"  :scope "provided"]
   [org.clojure/clojurescript "1.11.60" :scope "provided"]]
  
  :plugins
  [[lein-cljsbuild "1.1.7"]]

  :source-paths      ["src-clojure"]
  :java-source-paths ["src-java"]
  :test-paths        ["test-clojure"]

  :javac-options ["-target" "8" "-source" "8" "-bootclasspath" ~(str (or (System/getenv "JAVA8_HOME") (throw (Exception. "Please set JAVA8_HOME"))) "/jre/lib/rt.jar")]
  :jvm-opts ["-ea"]

  :profiles
  {:1.9
   {:dependencies 
    [[org.clojure/clojure       "1.9.0"   :scope "provided"]
     [org.clojure/clojurescript "1.9.946" :scope "provided"]]}}
   
  :deploy-repositories
  {"clojars"
   {:url "https://clojars.org/repo"
    :username "tonsky"
    :password :env/clojars_token
    :sign-releases false}})
