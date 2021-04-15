(ns talk.ws
  "Parse ws messages and forward to `in` with backpressure.
   Send ws messages asynchronously from `out-sub`."
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! put! close!]]
            [clojure.spec.alpha :as s]
            [talk.util :refer [on]]
            [talk.http :refer [->Connection]])
  (:import (io.netty.channel ChannelHandlerContext
                             SimpleChannelInboundHandler ChannelFutureListener ChannelHandler)
           (io.netty.handler.codec.http.websocketx
             TextWebSocketFrame CorruptedWebSocketFrameException WebSocketFrame
             WebSocketServerProtocolHandler$HandshakeComplete BinaryWebSocketFrame)
           (io.netty.handler.codec TooLongFrameException)
           (io.netty.buffer Unpooled)))

(s/def :plain/text string?)
(s/def :plain/data bytes?)
(s/def ::Text (s/keys :req-un [:talk.server/channel :plain/text]))
(s/def ::Binary (s/keys :req-un [:talk.server/channel :plain/data]))

(defrecord Text [channel text]
  Object
  (toString [r]
    (let [len (count text) long? (> len 10)]
      (str "<Text \"" (if long? (str (subs text 0 10) "...") text)
        "\" (" len " chars) on " (on r) \>))))
(defrecord Binary [channel data]
  Object
  (toString [r] (str "<Binary (" (alength data) " bytes on " (on r) \>)))

(defprotocol Unframe
  (unframe [this id]))
(extend-protocol Unframe
  TextWebSocketFrame
  (unframe [this id] (->Text id (.text this)))
  BinaryWebSocketFrame
  (unframe [this id]
    (let [content (.content this)
          data (byte-array (.readableBytes content))]
      (.getBytes content 0 data)
      (->Binary id data))))
(defprotocol Frame
  (frame [this]))
(extend-protocol Frame
  Text
  (frame [this] (TextWebSocketFrame. ^String (:text this)))
  Binary
  (frame [this] (BinaryWebSocketFrame. (Unpooled/wrappedBuffer ^bytes (:data this)))))

(defn send! [^ChannelHandlerContext ctx out-sub msg]
  (when msg ; async/take! passes nil if out-sub closed
    (let [ch (.channel ctx)
          id (.id ch)
          fr (try (frame msg)
                  (catch IllegalArgumentException e
                    (log/error "Unable to send this message type. Is it a record?" msg e)))
          take! #(async/take! out-sub (partial send! ctx out-sub))]
      (if fr
        (-> (.writeAndFlush ch fr)
            (.addListener
              (reify ChannelFutureListener
                (operationComplete [_ f]
                  (when (.isCancelled f)
                    (log/info "Cancelled message" msg "to" id))
                  (when-not (.isSuccess f)
                    (log/error "Send error for" msg "to" id (.cause f)))
                  #_(log/info "ChannelFutureListener")
                  (take!))))) ; facilitate backpressure
        (take!))))) ; even when previous message unsendable

(defn ^ChannelHandler handler
  "Forward incoming text messages to `in`.
   Send outgoing messages from `out-sub`.
   Both asynchronously and with backpressure."
  ; FIXME not taking advantage of zero-copy, but somewhat protected by backpressure.
  ; Copying means twice the memory is temporarily needed, until netty bytebuf released.
  ; Limited by needing to fit in (half of available) memory because of WebSocketFrameAggregator.
  ; Benefit is application not needing to worry about manual memory management...
  ; Contemplate repurposing or reimplementing simpler MixedAttribute to aggregate to memory vs disk depending on size (and turning off WSFA)
  [{:keys [in clients] :as opts}]
  (log/debug "Starting ws handler")
  (proxy [SimpleChannelInboundHandler] [WebSocketFrame]
    (userEventTriggered [^ChannelHandlerContext ctx evt]
      ; TODO propagate other user events?
      ; https://stackoverflow.com/a/36421052/780743
      (when (instance? WebSocketServerProtocolHandler$HandshakeComplete evt) ; TODO .selectedSubprotocol
        (let [ch (.channel ctx)
              id (.id ch)
              out-sub (get-in @clients [id :out-sub])]
          (swap! clients update id assoc :type :ws)
          (when-not (put! in (->Connection id :ws))
            (log/error "Unable to report connection upgrade because in chan is closed"))
          ; first take!, see send! for subsequent
          (if out-sub
            (async/take! out-sub (partial send! ctx out-sub))
            (log/error "No out-sub channel found")))))
    (channelRead0 [^ChannelHandlerContext ctx ^WebSocketFrame frame]
      ; Should already be off from http handler channelActive:
      #_(-> ctx .channel .config (.setAutoRead false))
      (let [ch (.channel ctx)
            id (.id ch)]
        (if-let [cnv (try (unframe frame id) ; Should already be aggregated
                          (catch IllegalArgumentException e
                            (log/info "Dropped incoming websocket message because unrecognised type" e)))]
          ; http://cdn.cognitect.com/presentations/2014/insidechannels.pdf
          ; https://github.com/loganpowell/cljs-guides/blob/master/src/guides/core-async-basics.md
          ; https://clojure.org/guides/core_async_go
          ; put! will throw AssertionError if >1024 requests queue up
          ; Netty prefers async everywhere, which is why I'm not using >!!
          (when-not (put! in cnv #(if % (.read ctx)))
            (log/error "Dropped incoming websocket message because in chan is closed" cnv)
            ; TODO do something about closed in chan? Shutdown?
            (.close ctx))
          (.read ctx))))
    (exceptionCaught [^ChannelHandlerContext ctx
                      ^Throwable cause]
      (condp instance? cause
        ; Actually when max *message* length is exceeded:
        TooLongFrameException
        (do (log/warn (type cause) (.getMessage cause))
            ; unblock backpressure
            (.read ctx))
        ; Max frame length exceeded:
        CorruptedWebSocketFrameException
        (do (log/warn (type cause) (.getMessage cause))
            (.read ctx))
        ; else
        (do (log/error "Error in websocket handler" cause)
            (.close ctx))))))