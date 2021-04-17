(ns talk.util
  "Edited highlights from jdf/comfort, to avoid dep."
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

(defn unsupported [^String msg] (throw (UnsupportedOperationException. msg)))

(defn wrap-channel
  "Make io.netty.channel.Channel look like a clojure map of the Channel's AttributeMap.
   Only supports plain keyword keys."
  [^Channel channel]
  (reify
    ILookup
    (valAt [_ k]
      (.attr channel (attribute-key k)))
    (valAt [this k default]
      (or (.valAt this k) default))
    Associative
    (containsKey [_ k]
      (boolean (.hasAttr channel (attribute-key k))))
    (entryAt [_ k]
      (.get (.attr channel (attribute-key k))))
    (assoc [_ k v]
      (.set (.attr channel (attribute-key k)) v))))

(defn wrap-channel-group
  "Make io.netty.channel.group.ChannelGroup look like a clojure map of `ChannelId` -> `Channel`.
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
      (unsupported "This is a wrapped io.netty.channel.group.ChannelGroup."))
    Associative
    (containsKey [_ k]
      (boolean (.find channel-group k)))
    (entryAt [_ k]
      (some-> (.find channel-group k) wrap-channel))
    (assoc [_ k v]
      (unsupported "This is a wrapped io.netty.channel.group.ChannelGroup."))))
    ;IObj
    ; (withMeta [_ _] (unsupported)))))
    ;IFn ; holy crap too many methods