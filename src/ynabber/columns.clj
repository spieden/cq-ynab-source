(ns ynabber.columns
  (:require [hashp.core]
            [cheshire.core :as json])
  (:import (io.cloudquery.types JSONType UUIDType)
           (java.nio.charset StandardCharsets)
           (java.time LocalDate LocalDateTime ZoneOffset)
           (java.util UUID)
           (org.apache.arrow.vector.types DateUnit TimeUnit)
           (org.apache.arrow.vector.types.pojo ArrowType$Bool ArrowType$Date ArrowType$Int ArrowType$Timestamp ArrowType$Utf8)
           (io.cloudquery.schema Column ColumnResolver)))

(defn json-type->arrow
  [{:keys [type format]}]
  (condp = [type format]
    ["string" nil] ArrowType$Utf8/INSTANCE
    ["string" "date"] (ArrowType$Date. DateUnit/DAY)
    ["string" "date-time"] (ArrowType$Timestamp. TimeUnit/MILLISECOND (str ZoneOffset/UTC))
    ["string" "uuid"] (UUIDType.)
    ["boolean" nil] ArrowType$Bool/INSTANCE
    ["integer" "int32"] (ArrowType$Int. 32 true)
    ["integer" "int64"] (ArrowType$Int. 64 true)
    ["array" nil] JSONType/INSTANCE
    ["object" nil] JSONType/INSTANCE))

(defn coerce
  [arrow-type value]
  (condp instance? arrow-type
    ArrowType$Utf8 value
    ArrowType$Date (-> value
                       (LocalDate/parse)
                       (.toEpochDay)
                       (Integer/valueOf))
    ArrowType$Timestamp (-> value
                            (LocalDateTime/parse)
                            (.atZone ZoneOffset/UTC)
                            (.toEpochSecond)
                            (* 1000))
    UUIDType (UUID/fromString value)
    ArrowType$Int value
    ArrowType$Bool value
    JSONType (.getBytes (json/encode value)
                        StandardCharsets/UTF_8)))

(def column-resolver
  (reify ColumnResolver
    (resolve [_ _ resource column]
      (let [col-name (.getName column)]
        (.set resource
              col-name
              (coerce (.getType column)
                      (get (.getItem resource)
                           (keyword col-name))))))))

(defn prop->column
  [[name-kw json-schema]]
  (-> (Column/builder)
      (.name (name name-kw))
      (.type (json-type->arrow json-schema))
      (.resolver column-resolver)
      (.primaryKey (= :id name-kw))
      (.build)))

(def record-schema
  [:map
   [:type [:= "object"]]
   [:properties [:map-of :keyword [:map [:type :string]]]]])

(defn object-schema->columns
  "returns a list of columns for the given object"
  {:malli/schema [:=>
                  [:cat record-schema]
                  [:vector [:fn #(instance? Column %)]]]}
  [json-schema]
  (mapv prop->column
        (:properties json-schema)))

(comment
  (require 'ynabber.schemas)
  (require 'malli.instrument)
  (malli.instrument/collect!)
  *e
  (malli.instrument/instrument!)

  (object-schema->columns (ynabber.schemas/extract-flat :CategoryGroupWithCategories))
  (object-schema->columns (ynabber.schemas/extract-flat :TransactionDetail))

  (malli.core/explain record-schema
                      {:type "object"
                       :properties {:foo {:type "string"}}})

  )
