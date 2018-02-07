(ns onyx-progress.watcher
  (:require [onyx.api]
            [onyx.log.curator :as zk]
            [clojure.core.async :refer [chan close! sliding-buffer mult untap-all tap >!!]]
            [onyx.compression.nippy :refer [zookeeper-compress
                                            zookeeper-decompress]]
            [com.stuartsierra.component :as component]
            [onyx-progress.lifecycle :refer [*onyx-job-progress-path*]]
            [zoo-routing.core :refer [start-watcher]]
            [zoo-routing.routes :as routes]))

(defn update-progress [state version job-id task value]
  (if (> (get-in state [:versions job-id] -1) version)
    state
    (-> state
        (assoc-in [:versions job-id] version)
        (assoc-in [:jobs job-id task] value))))

(defprotocol ISubscribe
  (subscribe [this ch] "subscribe chan"))

(defrecord ProgressWatcher [client-config watcher conn evt-ch mult-evt-ch]
  component/Lifecycle
  (start [this]
    (merge
      this
      (when-not (:started? this)
        (let [versions (atom {})
              evt-ch (chan (sliding-buffer 1000))
              mult-evt-ch (mult evt-ch)
              conn (zk/connect (:zookeeper/address client-config))
              progress-router (routes/prepare-route (str *onyx-job-progress-path* "/" (:onyx/tenancy-id client-config) "/:job-id/:task-ns/:task"))
              watcher (start-watcher
                        conn
                        (fn [evt]
                          (when-let [evt (routes/route-request evt progress-router)]
                            (let [{:keys [body stat route-params]} evt
                                  {:keys [version]} stat
                                  {:keys [job-id task-ns task]} route-params
                                  task-id (keyword (str (.substring task-ns 1) "/" task))]
                              (when (> version (get @versions job-id -1))
                                (swap! versions assoc job-id version)
                                (>!! evt-ch {:task     task-id
                                             :progress (zookeeper-decompress body)})))))
                        {:root (str *onyx-job-progress-path* "/" (:onyx/tenancy-id client-config))})]
          {:conn        conn
           :watcher     watcher
           :evt-ch      evt-ch
           :mult-evt-ch mult-evt-ch
           :started?    true}))))
  (stop [this]
    (merge
      this
      (when (:started? this)
        {:started?    false
         :state       nil
         :mult-evt-ch nil
         :evt-ch      (do (close! evt-ch) nil)
         :watcher     (do (watcher) nil)
         :conn        (do (zk/close conn) nil)})))

  ISubscribe
  (subscribe [this ch]
    (tap mult-evt-ch ch)))

(defn make-watcher [client-config]
  (map->ProgressWatcher {:client-config client-config}))
