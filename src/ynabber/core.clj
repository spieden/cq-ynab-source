(ns ynabber.core
  (:require [ynabber.columns :as cols]
            [ynabber.schemas :as schemas]
            [ynabber.client :as client]
            [martian.core :as martian])
  (:import (io.cloudquery.messages WriteMessage)
           (io.cloudquery.plugin BackendOptions NewClientOptions Plugin)
           (io.cloudquery.scheduler Scheduler)
           (io.cloudquery.schema ClientMeta Table TableResolver)
           (io.cloudquery.server PluginServe)
           (io.grpc.stub StreamObserver)
           (java.util List)))

(def plugin-name "ynabber-cq")
(def plugin-version "v0.0.1")

(def budget-id (System/getenv "BUDGET_ID") )
(def token (System/getenv "YNAB_TOKEN"))

(def tables
  {:transactions {:request :get-transactions
                  :items-key :transactions
                  :schema :TransactionDetail}
   :category_groups {:request :get-categories
                     :items-key :category_groups
                     :schema :CategoryGroupWithCategories}})

(defn table-resolver
  [request items-key]
  (reify TableResolver
    (resolve [_ client parent stream]
      (let [response (martian/response-for (:martian-client client)
                                           request
                                           {:budget-id budget-id})
            records (-> response :body :data items-key)]
        (doseq [record records]
          (.write stream record))))))

(defn table
  [[name-kw {:keys [request items-key schema]}]]
  (let [object-schema (schemas/extract-flat schema)
        resolver (table-resolver request items-key)]
    (-> (Table/builder)
        (.name (name name-kw))
        (.columns (cols/object-schema->columns object-schema))
        (.resolver resolver)
        (.build))))

(defrecord Client [martian-client]
  ClientMeta
  (getId [_] plugin-name)
  (^void write [_ ^WriteMessage message]
    (throw (ex-info "Asked to write a message!?"
                    {:message message}))))

(defn do-sync [client
               logger
               tables
               include-list
               skip-list
               skip-dependent-tables?
               deterministic-cq-id?
               backend-options
               sync-stream]
  (-> (Scheduler/builder)
      (.client client)
      ; normally filtering is applied here
      (.tables tables)
      (.syncStream sync-stream)
      (.deterministicCqId deterministic-cq-id?)
      ; there's also a pre-initialized one of these in a protected member
      (.logger logger)
      (.concurrency 2)
      (.build)
      (.sync)))

(def plugin
  (proxy [Plugin] [plugin-name plugin-version]
    (newClient ^ClientMeta [^String spec ^NewClientOptions options]
      (Client. (client/make-client token)))
    (tables ^List [^List include-list ^List skip-list ^Boolean skip-dependent-tables?]
      (mapv table tables))
    (sync ^void [^List include-list ^List skip-list ^Boolean skip-dependent-tables? ^Boolean deterministic-cq-id? ^BackendOptions backend-options ^StreamObserver sync-stream]
      (do-sync (proxy-super getClient)
               (proxy-super getLogger)
               (mapv table tables)
               include-list
               skip-list
               skip-dependent-tables?
               deterministic-cq-id?
               backend-options
               sync-stream))
    (read []
      (throw (ex-info "Asked to read!?" {})))
    (write [^WriteMessage message]
      (throw (ex-info "Asked to write!?" {})))
    (close [])))

(defn -main [& args]
  (-> (PluginServe/builder)
      (.plugin plugin)
      (.args (into-array String args))
      (.build)
      (.Serve)
      (System/exit)))
