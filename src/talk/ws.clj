(ns talk.ws
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! put! close!]])
  (:import (io.netty.channel ChannelHandlerContext
                             SimpleChannelInboundHandler ChannelFutureListener ChannelHandler)
           (io.netty.handler.codec.http.websocketx TextWebSocketFrame
                                                   CorruptedWebSocketFrameException)
           (io.netty.handler.codec TooLongFrameException)))

(defn ^ChannelHandler handler
  "Register websocket channel opening, and forward incoming text messages to `in` core.async chan.
   Server returns `clients` map atom which can be watched and enriched with additional metadata in application."
  [channel-group clients in]
  (proxy [SimpleChannelInboundHandler] []
    (channelActive [^ChannelHandlerContext ctx]
      (let [ch (.channel ctx)
            id (.id ch)
            cf (.closeFuture ch)]
        (try (.add channel-group ch)
             (swap! clients assoc id {:addr (.remoteAddress ch)})
             (when-not (>!! in [id true])
               (log/error "Unable to report connection because in chan is closed"))
             (.addListener cf (proxy [ChannelFutureListener] []
                                (operationComplete [_]
                                  (swap! clients dissoc id)
                                  (when-not (>!! in [id false])
                                    (log/error "Unable to report disconnection because in chan is closed")))))
             (catch Exception e
               (log/error "Unable to register channel" ch e)
               (throw e)))
        (.fireChannelActive ctx)))
    (channelRead0 [^ChannelHandlerContext ctx
                   ^TextWebSocketFrame frame]
      (let [ch (.channel ctx)
            id (.id ch)
            text (.text frame)]
        #_(log/debug "received" (count (.text frame)) "characters from"
            (.remoteAddress ch) "on channel id" (.id ch))
        ; http://cdn.cognitect.com/presentations/2014/insidechannels.pdf
        ; https://github.com/loganpowell/cljs-guides/blob/master/src/guides/core-async-basics.md
        ; https://clojure.org/guides/core_async_go
        (when-not (>!! in [id text]) ; `>!!` rather than `put!` may reveal need for buffering on `in`
          (log/error "Dropped incoming message because in chan is closed" text))))
    (exceptionCaught [^ChannelHandlerContext ctx
                      ^Throwable cause]
      (condp instance? cause
        ; Actually when max *message* length is exceeded:
        TooLongFrameException (log/warn (type cause) (.getMessage cause))
        ; Max frame length exceeded:
        CorruptedWebSocketFrameException (log/warn (type cause) (.getMessage cause))
        (log/error cause)))))