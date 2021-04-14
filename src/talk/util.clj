(ns talk.util
  "Edited highlights from jdf/comfort, to avoid dep."
  (:import (io.netty.channel ChannelId ChannelHandlerContext)
           (io.netty.handler.codec.http HttpRequest)))

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
  (ess [this] (-> this .channel .id .asShortText))
  ChannelId
  (ess [this] (.asShortText this))
  HttpRequest
  (ess [this] (str (.method this) \space (.uri this)))
  ;HttpResponse
  ;(ess [this] ())
  Object
  (ess [this] (.toString this)))