(ns talk.ws
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! put! close!]])
  (:import (io.netty.channel ChannelHandlerContext
                             SimpleChannelInboundHandler ChannelFutureListener ChannelHandler)
           (io.netty.handler.codec.http.websocketx
             TextWebSocketFrame CorruptedWebSocketFrameException WebSocketFrame
             WebSocketServerProtocolHandler$HandshakeComplete)
           (io.netty.handler.codec TooLongFrameException)))

(defn send! [^ChannelHandlerContext ctx out-sub {:keys [^String text] :as msg}]
  (if msg
    (let [ch (.channel ctx)
          id (.id ch)
          ; TODO accommodate BinaryWebSocketFrame
          cf (.writeAndFlush ch (TextWebSocketFrame. text))]
      #_(log/info "send!" msg)
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
   Send outgoing text messages from `out-sub`."
  [{:keys [in type clients] :as admin}]
  (proxy [SimpleChannelInboundHandler] [WebSocketFrame]
    (userEventTriggered [^ChannelHandlerContext ctx evt]
      (when (instance? WebSocketServerProtocolHandler$HandshakeComplete evt)
        (let [ch (.channel ctx)
              id (.id ch)
              out-sub (get-in @clients [id :out-sub])]
          (swap! clients update id assoc :type type)
          #_(log/info "clients in userEventTriggered" clients)
          (async/take! out-sub (partial send! ctx out-sub))))) ; first take!, see send! for subsequent
        ; TODO propagate other user events??
    (channelRead0 [^ChannelHandlerContext ctx
                   ^WebSocketFrame frame]
      ; facilitate backpressure on subsequent reads; requires .read see branches below
      (-> ctx .channel .config (.setAutoRead false))
      (let [ch (.channel ctx)
            id (.id ch)]
        (if (instance? TextWebSocketFrame frame)
          (let [text (.text ^TextWebSocketFrame frame)]
            #_(log/debug "received" (count (.text frame)) "characters from"
                (.remoteAddress ch) "on channel id" (.id ch))
            ; http://cdn.cognitect.com/presentations/2014/insidechannels.pdf
            ; https://github.com/loganpowell/cljs-guides/blob/master/src/guides/core-async-basics.md
            ; https://clojure.org/guides/core_async_go
            ; put! will throw AssertionError if >1024 requests queue up
            ; Netty prefers async everywhere, which is why I'm not using >!!
            (when-not (put! in {:ch id :text text}
                        (fn [val]
                          #_(log/info "returned from ws put!")
                          (if val
                            (.read ctx) ; because autoRead is false
                            (log/error "Dropped incoming websocket message because in chan is closed"))))
              (log/error "Dropped incoming websocket message because in chan is closed" text)))
          ; TODO do something about closed in chan? Shutdown?
          (do (log/info "Dropped incoming websocket message because not text")
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