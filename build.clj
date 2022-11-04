(ns build
  (:require [clojure.tools.build.api :as b]))

(def basis (b/create-basis {:project "deps.edn"}))

(defn clean
  "Cleans the target path."
  [_]
  (b/delete {:path "target"}))

(defn java
  "Compiles the java classes under `src/java`."
  [_]
  (b/javac {:src-dirs ["src-java"]
            :class-dir "target/classes"
            :basis basis}))

(comment
  (java nil)
  )
