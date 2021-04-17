(ns talk.util
  (:require [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]])
  (:import (io.netty.channel ChannelId ChannelHandlerContext Channel)
           (io.netty.handler.codec.http HttpRequest HttpResponse DefaultHttpContent HttpContent DefaultFullHttpResponse LastHttpContent)
           (io.netty.util AttributeKey)
           (io.netty.channel.group ChannelGroup)
           (clojure.lang IPersistentMap ILookup IPersistentCollection Counted Seqable Associative IObj MapEntry)
           (io.netty.handler.codec.http.multipart MixedAttribute InterfaceHttpPostRequestDecoder HttpPostRequestDecoder$NotEnoughDataDecoderException HttpPostRequestDecoder$EndOfDataDecoderException MixedFileUpload)
           (java.nio.charset Charset)))

; Copy-past from jdf/comfort to avoid dep

(defn retag
  "spec convenience"
  [gen-v tag] gen-v)

(defn briefly
  "Truncate string"
  ([clip comment] (cond (nil? comment) nil
                        (<= (count comment) clip) comment
                        :else (str (subs comment 0 clip) "...")))
  ([comment] (briefly 20 comment)))

; "Essentials" string renderer implementations

(defn spp [v] (with-out-str (pprint v)))

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
  (ess [this] (str (.protocolVersion this) \  (.method this) \  (.uri this) \newline
                (spp (into {} (.headers this)))))
  HttpContent
  (ess [this])
  DefaultFullHttpResponse
  (ess [this] (str (-> this .protocolVersion .toString) \  (-> this .status .toString) \newline
                (spp (into {} (.headers this)))
                (.decoderResult this)))
  Object
  (ess [this] (.toString this))
  nil
  (ess [this] "nil?!"))

; Wrappers for Channel and ChannelGroup to pretend to be clojure maps

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
    (assoc [this k v]
      ; NB Only to allow assoc-in and update-in to work!
      this)))
    ;IObj
    ; (withMeta [_ _] (unsupported)))))
    ;IFn ; holy crap too many methods

; Allow simple uploads

(defn fake-decoder
  "All I want is to be able to receive chunked plain POST/PUT/PATCH bodies!
   Return a single FileUpload. Use content-type: application/octet-stream for binary."
  [^String contentType size ^long limitSize ^Charset charset]
  (let [mfu (MixedFileUpload. "payload" "" contentType nil charset size limitSize)
        delivered? (atom false)]
    (reify InterfaceHttpPostRequestDecoder
      (isMultipart [_] false)
      (setDiscardThreshold [_ _] (unsupported))
      (getDiscardThreshold [_] (unsupported))
      (getBodyHttpDatas [_]
        (if (.isCompleted mfu)
          [mfu]
          (throw (HttpPostRequestDecoder$NotEnoughDataDecoderException.
                   (str "Need more chunks " name)))))
      ; Only proposing to collect one.
      (getBodyHttpDatas [this _]
        (.getBodyHttpDatas this))
      (getBodyHttpData [this _]
        (.getBodyHttpDatas this))
      (offer [_ con]
        ; TODO Double check this handles refcounts properly
        (.addContent mfu (.content con) (instance? LastHttpContent con)))
      (hasNext [_]
        (and (not @delivered?) (.isCompleted mfu)))
      (next [_]
        (if (.isCompleted mfu)
          (if @delivered?
            (throw (HttpPostRequestDecoder$EndOfDataDecoderException.))
            (and (reset! delivered? true) mfu))))
      (currentPartialHttpData [_]
        mfu)
      (destroy [_]
        (.delete mfu)
        (.release mfu)
        nil)
      (cleanFiles [_] (unsupported))
      (removeHttpDataFromClean [_ _] (unsupported)))))