(defproject onyx-progress "0.1.0"
  :description "Progress monitoring through Zookeeper"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.onyxplatform/onyx "0.12.6" :exclusions [com.google.guava/guava
                                                              org.apache.curator/curator-framework
                                                              commons-logging
                                                              org.slf4j/slf4j-api]]
                 [org.apache.curator/curator-framework "4.0.0" :exclusions [org.slf4j/slf4j-api
                                                                            org.apache.zookeeper/zookeeper]]
                 [org.slf4j/slf4j-api "1.7.25"]
                 [zoo-routing "0.1.2" :exclusions [log4j
                                                   org.clojure/tools.reader
                                                   org.apache.zookeeper/zookeeper
                                                   org.clojure/core.async]]
                 ])
