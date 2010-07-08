(ns org.rathore.amit.medusa.core
  (:use org.rathore.amit.utils.clojure)
  (:import java.util.concurrent.ExecutorService
           java.util.concurrent.Executors))

(def THREAD-TIMEOUT-MILLIS 20000)
(def *SUPERVISOR-ENABLED* true)
(def SUPERVISE-EVERY-MILLIS 10000)
(def MEDUSA-THREADPOOL-SIZE (* 3 (.availableProcessors (Runtime/getRuntime))))
(def THREADPOOL (Executors/newFixedThreadPool MEDUSA-THREADPOOL-SIZE))

(def running-futures (ref {}))

(defn claim-thread [future-id]
  (let [thread-info {:thread (Thread/currentThread) :future-id future-id :started (System/currentTimeMillis)}]
    (dosync (alter running-futures assoc future-id thread-info))))

(defn mark-completion [future-id]
  (dosync (alter running-futures dissoc future-id)))

(defn medusa-future-thunk [future-id thunk]
  (let [work (fn []
               (claim-thread future-id)
               (thunk)
               (mark-completion future-id))]
    (.submit THREADPOOL work)))

(defn preempt-medusa-future [[future-id {:keys [thread]}]]
  (.interrupt thread)
  (mark-completion future-id))

(defn running-over? [[_ {:keys [started]}]]
  (> (- (System/currentTimeMillis) started) THREAD-TIMEOUT-MILLIS))

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
    (future (supervise SUPERVISE-EVERY-MILLIS))))

(defn shutdown-medusa []
  (.shutdown THREADPOOL))