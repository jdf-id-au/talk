(ns talk.util
  "Edited highlights from jdf/comfort, to avoid dep."
  (:import (io.netty.channel ChannelId DefaultChannelId ChannelHandlerContext)
           (io.netty.handler.codec.http DefaultHttpRequest DefaultHttpResponse)))

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
  DefaultChannelId
  (ess [this] (.asShortText this))
  DefaultHttpRequest
  (ess [this] (str (.method this) \space (.uri this)))
  ;DefaultHttpResponse
  ;(ess [this] ())
  Object
  (ess [this] (.toString this)))