(ns talk.util
  "Edited highlights from jdf/comfort, to avoid dep."
  (:import (io.netty.channel ChannelId ChannelHandlerContext Channel)
           (io.netty.handler.codec.http HttpRequest HttpResponse)
           (io.netty.util AttributeKey)))

(defn retag
  "spec convenience"
  [gen-v tag] gen-v)

(defn briefly
  "Truncate string"
  ([clip comment] (cond (nil? comment) nil
                        (<= (count comment) clip) comment
                        :else (str (subs comment 0 clip) "...")))
  ([comment] (briefly 20 comment)))

(defn on
  "Annotate on channel"
  [rec] (some-> ^ChannelId (:channel rec) .asShortText))

(defprotocol Loggable
  (ess [this] "Essential details as string"))

(extend-protocol Loggable
  ChannelHandlerContext
  (ess [this] (ess (.channel this)))
  Channel
  (ess [this] (ess (.id this)))
  ChannelId
  (ess [this] (.asShortText this))
  HttpRequest
  (ess [this] (str (.method this) \space (.uri this)))
  HttpResponse
  (ess [this] (str (.protocolVersion this) \space (.status this) \space (.headers this)))
  Object
  (ess [this] (.toString this))
  nil
  (ess [this] "nil?!"))

(defprotocol ChannelAtomic
  (-ch-assoc [this kvs])
  (ch-get [this k]
    "Treat Netty Channel a bit like a clojure map-in-atom via its AttributeMap interface.
     Like get but can't distinguish between 'contains nil' and 'not found'.")
  ;(ch-update [this k f])
  (-ch-dissoc [this ks]))

(defn ch-assoc
  "Treat Netty Channel a bit like a clojure map-in-atom via its AttributeMap interface.
   Like assoc but doesn't return map."
  ; workaround no varargs in defprotocol
  ([this k v] (-ch-assoc this (list [k v])))
  ([this k v & kvs]
   (assert (even? (count kvs)))
   (-ch-assoc this (cons [k v] (partition 2 kvs)))))

(defn ch-dissoc
  "Treat Netty Channel a bit like a clojure map-in-atom via its AttributeMap interface.
   Like dissoc but doesn't return map."
  ; workaround no varargs in defprotocol
  ([this & ks] (-ch-dissoc this ks)))

(defn attribute-key [kw]
  (AttributeKey/valueOf (name kw)))

(extend-protocol ChannelAtomic
  Channel
  (-ch-assoc [this kvs]
    (doseq [[k v] kvs] (.set (.attr this (attribute-key k)) v)))
  (ch-get [this k]
    (.get (.attr this (attribute-key k))))
  (-ch-dissoc [this ks]
    (doseq [k ks] (.set (.attr this (attribute-key k)) nil))))