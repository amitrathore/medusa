(ns org.rathore.amit.medusa.utils)

(defn random-uuid []
  (str (java.util.UUID/randomUUID)))

(defn threadpool-factory [thread-name-prefix]
  (proxy [java.util.concurrent.ThreadFactory] []
    (newThread [r]
      (let [t (Thread. r)]
        (.setName t (str thread-name-prefix (random-uuid)))
        t))))
