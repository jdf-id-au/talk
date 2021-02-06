(ns talk.ws
  "Parse ws messages and forward to `in` with backpressure.
   Send ws messages asynchronously from `out-sub`."
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! put! close!]]
            [clojure.spec.alpha :as s]
            #_[talk.server :as server :refer [ChannelInboundMessageHandler Aggregator accept]])
  (:import (io.netty.channel ChannelHandlerContext
                             SimpleChannelInboundHandler ChannelFutureListener ChannelHandler)
           (io.netty.handler.codec.http.websocketx
             TextWebSocketFrame CorruptedWebSocketFrameException WebSocketFrame
             WebSocketServerProtocolHandler$HandshakeComplete BinaryWebSocketFrame ContinuationWebSocketFrame)
           (io.netty.handler.codec TooLongFrameException)))

(s/def :plain/text string?)
(s/def :plain/data bytes?)

(s/def ::text (s/keys :req-un [:talk.api/ch :plain/text]))
(s/def ::binary (s/keys :req-un [:talk.api/ch :plain/data]))

(defn send! [^ChannelHandlerContext ctx out-sub {:keys [text data] :as msg}]
  (if msg
    (let [ch (.channel ctx)
          id (.id ch)
          cf (cond text (.writeAndFlush ch (TextWebSocketFrame. ^String text))
                   data (.writeAndFlush ch (BinaryWebSocketFrame. ^bytes data)))]
      (.addListener cf
        (reify ChannelFutureListener
          (operationComplete [_ f]
            (when (.isCancelled f)
              (log/info "Cancelled message" msg "to" id))
            (when-not (.isSuccess f)
              (log/error "Send error for" msg "to" id (.cause f)))
            (log/info "ChannelFutureListener")
            (async/take! out-sub (partial send! ctx out-sub)))))) ; facilitate backpressure
    #_(log/info "Out pub-sub closed.")))

(defn ^ChannelHandler handler
  "Forward incoming text messages to `in`.
   Send outgoing text messages from `out-sub`.
   Both asynchronously and with backpressure."
  ; FIXME not taking advantage of zero-copy, but somewhat protected by backpressure.
  ; Copying means twice the memory is temporarily needed, until netty bytebuf released.
  ; Limited by needing to fit in (half of available) memory because of WebSocketFrameAggregator.
  ; Benefit is application not needing to worry about manual memory management...
  [{:keys [in clients] :as opts}]
  (proxy [SimpleChannelInboundHandler] [WebSocketFrame]
    (userEventTriggered [^ChannelHandlerContext ctx evt]
      ; TODO propagate other user events?
      (when (instance? WebSocketServerProtocolHandler$HandshakeComplete evt)
        (let [ch (.channel ctx)
              id (.id ch)
              out-sub (get-in @clients [id :out-sub])]
          (swap! clients update id assoc :type :ws)
          ; first take!, see send! for subsequent
          (async/take! out-sub (partial send! ctx out-sub)))))
    (channelRead0 [^ChannelHandlerContext ctx ^WebSocketFrame frame]
      ; Should already be off from http handler channelActive:
      #_(-> ctx .channel .config (.setAutoRead false))
      (let [ch (.channel ctx)
            id (.id ch)]
        (condp instance? frame
          TextWebSocketFrame
          (let [text (.text ^TextWebSocketFrame frame)]
            ; http://cdn.cognitect.com/presentations/2014/insidechannels.pdf
            ; https://github.com/loganpowell/cljs-guides/blob/master/src/guides/core-async-basics.md
            ; https://clojure.org/guides/core_async_go
            ; put! will throw AssertionError if >1024 requests queue up
            ; Netty prefers async everywhere, which is why I'm not using >!!
            (when-not (put! in {:ch id :type ::text :text text} #(if % (.read ctx)))
              (log/error "Dropped incoming websocket message because in chan is closed" text)))
              ; TODO do something about closed in chan? Shutdown?
          BinaryWebSocketFrame
          (let [content (.content frame)
                data (byte-array (.readableBytes content))]
            (.getBytes content 0 data)
            (when-not (put! in {:ch id :type ::binary :data data} #(if % (.read ctx)))
              (log/error "Dropped incoming websocket message because in chan is closed" data)))
          (do (log/info "Dropped incoming websocket message because unrecognised type")
              (.read ctx)))))
    (exceptionCaught [^ChannelHandlerContext ctx
                      ^Throwable cause]
      (condp instance? cause
        ; Actually when max *message* length is exceeded:
        TooLongFrameException (log/warn (type cause) (.getMessage cause))
        ; Max frame length exceeded:
        CorruptedWebSocketFrameException (log/warn (type cause) (.getMessage cause))
        (do (log/error "Error in websocket handler" cause)
            (.close ctx))))))