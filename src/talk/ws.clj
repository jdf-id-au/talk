(ns talk.ws
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! put! close!]]
            [talk.common :as common])
  (:import (io.netty.channel ChannelHandlerContext
                             SimpleChannelInboundHandler ChannelFutureListener ChannelHandler)
           (io.netty.handler.codec.http.websocketx TextWebSocketFrame
                                                   CorruptedWebSocketFrameException WebSocketFrame)
           (io.netty.handler.codec TooLongFrameException)
           (java.net InetSocketAddress)
           (io.netty.channel.group ChannelGroup)))

(defn send! [^ChannelHandlerContext ctx out-sub {:keys [^String text] :as msg}]
  (if msg
    (let [ch (.channel ctx)
          id (.id ch)
          ; TODO accommodate BinaryWebSocketFrame
          cf (.writeAndFlush ch (TextWebSocketFrame. text))]
      (.addListener cf
        (reify ChannelFutureListener
          (operationComplete [_ f]
            (when (.isCancelled f)
              (log/info "Cancelled message" msg "to" id))
            (when-not (.isSuccess f)
              (log/error "Send error for" msg "to" id (.cause f)))
            (async/take! out-sub (partial send! ctx out-sub)))))) ; facilitate backpressure
    (log/error "Dropped outgoing message. Is sub closed?")))

; vs WebSocketFrame https://netty.io/4.1/xref/io/netty/example/http/websocketx/server/WebSocketFrameHandler.html
(defn ^ChannelHandler handler
  "Register websocket channel opening, and forward incoming text messages to `in` core.async chan.
   Server returns `clients` map atom which can be watched and enriched with additional metadata in application."
  [{:keys [in type] :as admin}]
  (proxy [SimpleChannelInboundHandler] [WebSocketFrame]
    (channelActive [^ChannelHandlerContext ctx] (common/track-channel ctx admin send!))
    (channelRead0 [^ChannelHandlerContext ctx
                   ^WebSocketFrame frame]
      ; facilitate backpressure on subsequent reads; requires (.read ch) see branches below
      (-> ctx .channel .config (.setAutoRead false))
      (let [ch (.channel ctx)
            id (.id ch)]
        (if (instance? TextWebSocketFrame frame)
          (let [text (.text frame)]
            #_(log/debug "received" (count (.text frame)) "characters from"
                (.remoteAddress ch) "on channel id" (.id ch))
            ; http://cdn.cognitect.com/presentations/2014/insidechannels.pdf
            ; https://github.com/loganpowell/cljs-guides/blob/master/src/guides/core-async-basics.md
            ; https://clojure.org/guides/core_async_go
            ; put! will throw AssertionError if >1024 requests queue up
            ; Netty prefers async everywhere, which is why I'm not using >!!
            (when-not (put! in {:ch id :text text}
                        (fn [val]
                          (if val
                            (.read ch) ; because autoRead is false
                            (log/error "Dropped incoming websocket message because in chan is closed"))))
              (log/error "Dropped incoming websocket message because in chan is closed" text)))
          ; TODO do something about closed in chan? Shutdown?
          (do (log/info "Dropped incoming websocket message because not text")
              (.read ch)))))
    (exceptionCaught [^ChannelHandlerContext ctx
                      ^Throwable cause]
      (condp instance? cause
        ; Actually when max *message* length is exceeded:
        TooLongFrameException (log/warn (type cause) (.getMessage cause))
        ; Max frame length exceeded:
        CorruptedWebSocketFrameException (log/warn (type cause) (.getMessage cause))
        (do (log/error "Error in websocket handler" cause)
            (.close ctx))))))