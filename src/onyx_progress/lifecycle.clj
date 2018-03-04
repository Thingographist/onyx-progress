(ns onyx-progress.lifecycle
  (:require [onyx.log.curator :as zk]
            [onyx.compression.nippy :refer [zookeeper-compress
                                            zookeeper-decompress]]))

(def ^:dynamic *onyx-job-progress-path* "/onyx-progress")

(defn before-task-start [{:keys [onyx.core/job-id
                                 onyx.core/tenancy-id
                                 onyx.core/task-id
                                 onyx.core/log
                                 onyx-progress.lifecycle/conn]}
                         lifecycle]
  (if (nil? conn)
    (let [conn (zk/connect (-> log :config :zookeeper/address))
          path (str *onyx-job-progress-path* "/" tenancy-id "/" job-id "/" task-id)
          progress-measures (zookeeper-compress {:in 0 :out 0})]
      (zk/create-all conn path :data progress-measures :persistent? true)
      {::node path
       ::conn conn})
    {}))

(defn after-task-stop [{:keys [onyx-progress.lifecycle/conn
                               onyx-progress.lifecycle/node]}
                       lifecycle]
  (if (some? conn)
    (do
      (zk/delete conn node)
      (zk/close conn)
      {::node nil
       ::conn nil})
    {}))

(defn update-progress [zk-progress-key]
  (fn [{:keys [onyx-progress.lifecycle/conn
               onyx-progress.lifecycle/node
               onyx.core/batch]}
       lifecycle]
    ;; NOTE batch maybe []
    (when (seq batch)
      (let [node-val (zk/data conn node)
            bytes (-> (:data node-val)
                      (zookeeper-decompress)
                      (update zk-progress-key + (count batch))
                      (zookeeper-compress))]
        (zk/set-data conn node bytes (get-in node-val [:stat :version]))))
    {}))

(def progress-calls
  {:lifecycle/before-task-start before-task-start
   :lifecycle/after-task-stop   after-task-stop
   :lifecycle/after-read-batch  (update-progress :in)
   :lifecycle/after-batch       (update-progress :out)})

(defn progress-lifecycle [task-name]
  [{:lifecycle/task  task-name
    :lifecycle/calls ::progress-calls}])
