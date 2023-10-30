(ns ynabber.client
  (:require [martian.hato :as martian-http]))

(def api-spec-resource "ynab_v1.openapi_v3.yaml")

(defn add-authentication-header [token]
  {:name ::add-authentication-header
   :enter (fn [ctx]
            (assoc-in ctx
                      [:request :headers "Authorization"]
                      (str "Bearer " token)))})

(defn make-client [token]
  (martian-http/bootstrap-openapi api-spec-resource
                                  {:interceptors (concat martian-http/hato-interceptors
                                                         [(add-authentication-header token)
                                                          martian-http/perform-request])}))
