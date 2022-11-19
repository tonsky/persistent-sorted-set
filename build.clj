(ns build
  (:require [clojure.tools.build.api :as b]))

(def basis
  (b/create-basis {:project "deps.edn"}))

(defn clean
  "Cleans `target`"
  [_]
  (b/delete {:path "target"}))

(defn java
  "Compiles `src/java` to `target/classes`"
  [_]
  ; (println "Compiling 'src/java' to 'target/classes'...")
  (b/javac {:src-dirs  ["src-java"]
            :class-dir "target/classes"
            :basis     basis}))

(comment
  (java nil))
