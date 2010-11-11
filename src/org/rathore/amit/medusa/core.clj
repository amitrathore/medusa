(ns org.rathore.amit.medusa.core
  (:use org.rathore.amit.utils.clojure
        org.rathore.amit.medusa.utils)
  (:import java.util.concurrent.ExecutorService
           java.util.concurrent.Executors))

(def *FUTURE-TIMEOUT-MILLIS* 20000)
(def *SUPERVISOR-ENABLED* true)
(def *SUPERVISE-EVERY-MILLIS* 10000)
(def *MEDUSA-THREADPOOL-SIZE* (* 3 (.availableProcessors (Runtime/getRuntime))))
(def THREADPOOL)

(def running-futures (ref {}))

(defn new-fixed-threadpool [size]
  (Executors/newFixedThreadPool size))

(defrunonce init-medusa 
  ([]
     (def THREADPOOL (new-fixed-threadpool *MEDUSA-THREADPOOL-SIZE*)))
  ([pool-size]
     (def THREADPOOL (new-fixed-threadpool pool-size))))

(defn claim-thread [future-id]
  (let [thread-info {:thread (Thread/currentThread) :future-id future-id :started (System/currentTimeMillis)}]
    (dosync (alter running-futures assoc future-id thread-info))))

(defn mark-completion [future-id]
  (dosync (alter running-futures dissoc future-id)))

(defn medusa-future-thunk [future-id thunk]
  (let [work (fn []
               (claim-thread future-id)
               (let [val (thunk)]
                 (mark-completion future-id)
                 val))]
    (.submit THREADPOOL work)))

(defmacro medusa-future [& body]
  `(medusa-future-thunk (random-uuid) (fn [] (do ~@body))))

(defn medusa-pmap [f coll]
  (let [seq-of-futures (doall (map #(medusa-future (f %)) coll))]
    (map (fn [java-future] (.get java-future)) seq-of-futures)))

(defn preempt-medusa-future [[future-id {:keys [thread]}]]
  (.interrupt thread)
  (mark-completion future-id))

(defn running-over? [[_ {:keys [started]}]]
  (> (- (System/currentTimeMillis) started) *FUTURE-TIMEOUT-MILLIS*))

(defn preempt-old-futures []
  (let [to-preempt (filter running-over? @running-futures)]
    (doseq [frs to-preempt]
      (preempt-medusa-future frs))))

(defn supervise [repeat-after-millis]
  (while true
    (preempt-old-futures)
    (Thread/sleep repeat-after-millis)))

(defrunonce start-supervisor []
  (if *SUPERVISOR-ENABLED*
    (future (supervise *SUPERVISE-EVERY-MILLIS*))))

(defn shutdown-medusa []
  (.shutdown THREADPOOL))

(defn number-of-queued-tasks []
  (.size (.getQueue THREADPOOL)))

(defn current-pool-size []
  (.getPoolSize THREADPOOL))

(defn completed-task-count []
  (.getCompletedTaskCount THREADPOOL))

(defn futures-count []
  (count @running-futures))
