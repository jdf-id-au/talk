(ns talk.util
  "Edited highlights from jdf/comfort, to avoid dep."
  (:require [clojure.tools.logging :as log])
  (:import (io.netty.channel ChannelId ChannelHandlerContext Channel)
           (io.netty.handler.codec.http HttpRequest HttpResponse)
           (io.netty.util AttributeKey)
           (io.netty.channel.group ChannelGroup)
           (clojure.lang IPersistentMap ILookup IPersistentCollection Counted Seqable Associative IObj MapEntry)))

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

(defn attribute-key [kw]
  (AttributeKey/valueOf (name kw)))

(defn unsupported
  ([] (throw (UnsupportedOperationException.)))
  ([^String msg] (throw (UnsupportedOperationException. msg))))

(defn wrap-channel
  "Make io.netty.channel.Channel look a bit like a clojure map of the Channel's AttributeMap.
   Only supports plain keyword keys. Doesn't distinguish between Attribute with nil value and absent attribute."
  [^Channel channel]
  (reify
    ILookup
    (valAt [_ k]
      (.get (.attr channel (attribute-key k))))
    (valAt [this k default]
      (or (.valAt this k) default))
    IPersistentMap ; extends Iterable, Associative, Counted;
    (assocEx [this k v]
      (if-not (.get (.attr channel (attribute-key k)))
        (.set (.attr channel (attribute-key k)) v)
        (throw (RuntimeException. "Key already present")))
      this)
    (without [this k]
      (.set (.attr channel (attribute-key k)) nil)
      this)
    ; Iterable:
    (iterator [_] (unsupported "This is a wrapped io.netty.channel.Channel. No iterator."))
    ;(forEach [_ _] (unsupported "This is a wrapped io.netty.channel.Channel. No forEach."))
    ;(spliterator [_] (unsupported "This is a wrapped io.netty.channel.Channel. No spliterator."))
    ; Associative:
    (containsKey [_ k]
      (boolean (.hasAttr channel (attribute-key k))))
    (entryAt [_ k]
      (.get (.attr channel (attribute-key k))))
    (assoc [this k v]
      (.set (.attr channel (attribute-key k)) v)
      this)
    ; Counted:
    (count [_] (unsupported "This is a wrapped io.netty.channel.Channel. No count."))))

(defn wrap-channel-group
  "Make io.netty.channel.group.ChannelGroup look a bit like a clojure map of `ChannelId` -> `Channel`.
   Each `Channel` is wrapped with `wrap-channel`."
  ; It's already a java.util.Set<io.netty.channel.Channel>.
  ; Don't want to bring in clj-commons/potemkin for def-map-type!
  [^ChannelGroup channel-group]
  ; Clojure interfaces which might be needed:
  ; IPersistentCollection IPersistentMap Counted Seqable ILookup Associative IObj IFn
  ; Important functions:
  ; get assoc dissoc keys meta with-meta
  (reify
    ;IPersistentCollection ; extends Seqable
    ;(count [_] (count (channel-group)))
    ;(cons [_ _] (unsupported))
    ;(empty [_] (unsupported))
    ;(equiv [_ _] (unsupported))
    ;IPersistentMap ; extends Iterable, Associative, Counted
    ;(assoc [_ _ _] (unsupported))
    ;(assocEx [_ _ _] (unsupported))
    ;(without [_ _] (unsupported))
    Counted
    (count [_]
      (.size channel-group))
    Seqable
    (seq [_]
      (seq (map (fn [^Channel ch] (MapEntry. (.id ch) (wrap-channel ch))) channel-group)))
    ILookup
    (valAt [_ k]
      (some-> (.find channel-group k) wrap-channel))
    (valAt [_ _ _]
      (unsupported "This is a wrapped io.netty.channel.group.ChannelGroup. Meaningless to give default."))
    Associative
    (containsKey [_ k]
      (boolean (.find channel-group k)))
    (entryAt [_ k]
      (some-> (.find channel-group k) wrap-channel))
    (assoc [_ k v]
      (unsupported "This is a wrapped io.netty.channel.group.ChannelGroup. Meaningless to assoc."))))
    ;IObj
    ; (withMeta [_ _] (unsupported)))))
    ;IFn ; holy crap too many methods