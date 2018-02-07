(ns onyx-progress.watcher-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [chan go go-loop <! <!!] :as async]
            [onyx.api]
            [onyx.tasks.seq]
            [onyx-progress.watcher :refer [make-watcher subscribe]]
            [onyx-progress.lifecycle :refer [progress-lifecycle]]
            [com.stuartsierra.component :as component]))

(defn execute-job [config job]
  (let [group (onyx.api/start-peer-group config)
        peers (onyx.api/start-peers 5 group)
        job (onyx.api/submit-job config job)]
    (onyx.api/await-job-completion config (:job-id job))
    (onyx.api/shutdown-peers peers)
    (onyx.api/shutdown-peer-group group)))

(deftest test-watcher
  "simple sting watcher"
  (let [tenancy-id "test-cluster"
        zookeeper-address "127.0.0.1:2188"
        config {:onyx/tenancy-id                                tenancy-id
                :zookeeper/address                              zookeeper-address
                :onyx.messaging/bind-addr                       "localhost"
                :onyx.messaging/impl                            :aeron
                :onyx.messaging/peer-port                       40200
                :onyx.peer/job-scheduler                        :onyx.job-scheduler/balanced
                :onyx.peer/storage.zk.insanely-allow-windowing? true}

        workflow (onyx.api/map-set-workflow->workflow {::input #{::output}})
        seq-task (:task
                   (onyx.tasks.seq/input-serialized
                     ::input
                     {:onyx/batch-size 10}
                     (shuffle (range 1000))))
        catalog [(:task-map seq-task)
                 {:onyx/name       ::output
                  :onyx/plugin     :onyx.peer.function/function
                  :onyx/medium     :function
                  :onyx/type       :output
                  :onyx/fn         :clojure.core/identity
                  :onyx/batch-size 20}]
        lifecycles (concat
                     (:lifecycles seq-task)
                     (progress-lifecycle ::input))

        env (onyx.api/start-env {:zookeeper/address     zookeeper-address
                                 :onyx/tenancy-id       tenancy-id
                                 :zookeeper/server?     true
                                 :zookeeper.server/port 2188})
        ch (chan)
        watcher (component/start (make-watcher config))]
    (subscribe watcher ch)
    (try
      (execute-job config
                   {:catalog        catalog
                    :workflow       workflow
                    :lifecycles     lifecycles
                    :task-scheduler :onyx.task-scheduler/balanced})
      (go
        (is
          (= {:task :onyx-progress.watcher-test/input :progress {:in 1000, :out 1000}}
             (last (<!! (async/into [] ch))))))
      (finally
        (component/stop watcher)
        (onyx.api/shutdown-env env)))))
