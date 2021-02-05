(ns talk.server
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]])
  (:import (io.netty.channel ChannelInitializer ChannelHandlerContext
                             ChannelInboundHandler ChannelFutureListener ChannelOption)
           (io.netty.channel.socket SocketChannel)
           (io.netty.handler.codec.http HttpServerCodec)
           (io.netty.handler.stream ChunkedWriteHandler)
           (io.netty.handler.codec.http.websocketx WebSocketServerProtocolHandler)
           (io.netty.util ReferenceCountUtil)
           (io.netty.channel.group ChannelGroup)
           (java.net InetSocketAddress)))

(defn track-channel
  "Register channel in `clients` map and report on `in` chan.
   Map entry is a map containing `type`, `out-sub` and `addr`, and can be updated.

   Usage:
   - Call from channelActive.
   - Detect websocket upgrade handshake, using userEventTriggered, and update `clients` map."
  [^ChannelHandlerContext ctx
   {:keys [^ChannelGroup channel-group clients in out-pub]}]
  (let [ch (.channel ctx)
        id (.id ch)
        cf (.closeFuture ch)
        out-sub (chan)]
    (try (.add channel-group ch)
         (async/sub out-pub id out-sub)
         (swap! clients assoc id
            :type :http ; changed in ws userEventTriggered
            :out-sub out-sub
            :addr (-> ch ^InetSocketAddress .remoteAddress .getAddress .toString))
         (when-not (put! in {:ch id :type :talk.api/connection :connected true})
           (log/error "Unable to report connection because in chan is closed"))
         (.addListener cf
           (reify ChannelFutureListener
             (operationComplete [_ _]
               (swap! clients dissoc id)
               (when-not (put! in {:ch id :type :talk.api/connection :connected false})
                 (log/error "Unable to report disconnection because in chan is closed")))))
         (catch Exception e
           (log/error "Unable to register channel" ch e)
           (throw e)))))

(defprotocol ChannelInboundMessageHandler
  "Naming convention adapted from `SimpleChannelInboundHandler`."
  (channelRead0 [msg broader-context]
    "Handle specific netty message type.")
  (offer [msg so-far broader-context]
    "Ask for msg to be aggregated into so-far. Return [status result broader-context]."))

; "Don't extend in lib if don't own both protocol and target type."
;(extend-protocol ChannelInboundMessageHandler
;  ; See talk.http and talk.ws. Anything else isn't handled so send to next handler in pipeline.
;  Object
;  (channelRead0 [_ _] false)
;  (offer [_ _ _])
;  nil
;  (channelRead0 [_ _]) ; i.e. nil
;  (offer [_ _ _]))

(defprotocol Aggregator
  (accept [so-far msg broader-context] "Attempt to aggregate processed msg into so-far."))

(defn aggregator
  "Aggregate from chunks chan into messages chan."
  [chunks messages]
  (go-loop [[bc msg] (<! chunks)
            so-far nil] ; TODO profile; contemplate transient/volatile/...?
    (if msg
      (let [[status result] (offer msg so-far bc)]
        (case (-> status name keyword) ; un-namespace the status keyword
          (:start :ok) (recur (<! chunks) result)
          (:finish) (if (>! messages result) (recur (<! chunks) nil)
                      (log/info "messages chan closed, dropping" result))
          (log/warn "Aggregation failed: " (or status "(no status code)")
            "\nMessage:" msg "\nAggregator:" so-far)))
      (log/info "chunks chan closed, managed to make" (or so-far "nothing")))))

(defn ^ChannelInboundHandler aggregate-and-handle
  "Track netty channels and clients.
   Pass netty messages to handlers.
   Also pass a chan which can be used for aggregation of messages on each netty channel."
  [opts]
  (let [chunks (chan)
        messages (chan)
        _ (aggregator chunks messages) ; TODO make use of return channel value?
        ; TODO contemplate/measure resource usage from two chans per channel; probably light enough?
        opts (assoc opts :chunks chunks :messages messages)]
    (reify ChannelInboundHandler
      ; TODO ^nil, ^void, or no hint?
      (^void channelRegistered [_ ^ChannelHandlerContext ctx] (.fireChannelRegistered ctx))
      (^void channelUnregistered [_ ^ChannelHandlerContext ctx] (.fireChannelUnregistered ctx))
      (^void channelActive [_ ^ChannelHandlerContext ctx]
        ; Facilitate backpressure on subsequent reads. Requires manual `(.read ctx)` to indicate readiness.
        (-> ctx .channel .config (-> (.setAutoRead false)
        ; May be needed for response from outside netty event loop:
        ; https://stackoverflow.com/a/48128514/780743
                                     (.setOption ChannelOption/ALLOW_HALF_CLOSURE true)))
        (track-channel ctx opts)
        (.read ctx))
      (^void channelInactive [_ ^ChannelHandlerContext ctx] (.fireChannelInactive ctx))
      (^void channelRead [_ ^ChannelHandlerContext ctx msg]
        ; Adapted from SimpleChannelInboundHandler
        #_(let [release? (volatile! true)]
            (try (when-not (channelRead0 msg ctx)
                   (vreset! release? false)
                   (.fireChannelRead ctx msg))
                 (finally (when @release? (ReferenceCountUtil/release msg)))))
        (try (channelRead0 msg (assoc opts :ctx ctx)) ; channelRead0 needs to release msg when done
          (catch Exception e (ReferenceCountUtil/release msg) (throw e)))
        nil)
      (^void channelReadComplete [_ ^ChannelHandlerContext ctx] (.fireChannelReadComplete ctx))
      (^void userEventTriggered [_ ^ChannelHandlerContext ctx evt] (.fireUserEventTriggered ctx evt))
      (^void channelWritabilityChanged [_ ^ChannelHandlerContext ctx] (.fireChannelWritabilityChanged ctx))
      (^void exceptionCaught [_ ^ChannelHandlerContext ctx ^Throwable cause] (.fireExceptionCaught ctx cause)))))
      ; ChannelHandler methods
      ;(handlerAdded [_ ^ChannelHandlerContext ctx])
      ;(handlerRemoved [_ ^ChannelHandlerContext ctx])))

(defn pipeline
  [^String ws-path
   {:keys [^int handshake-timeout ^int max-frame-size] :as opts}]
  (proxy [ChannelInitializer] []
    (initChannel [^SocketChannel ch]
      (doto (.pipeline ch)
        ; TODO could add handlers selectively according to need (from within the channel)
        (.addLast "http" (HttpServerCodec.)) ; TODO could tune chunk size
        ; less network but more memory
        #_(.addLast "http-compr" (HttpContentCompressor.))
        #_(.addLast "http-decompr" (HttpContentDecompressor.))
        ; inbound only https://stackoverflow.com/a/38947978/780743

        ; TODO replace with functionality in main handler
        #_(.addLast "http-agg" (HttpObjectAggregator. max-content-length))
        ; FIXME obviously DDOS risk so conditionally add to pipeline once auth'd?
        #_(.addLast "http-disk-agg" (DiskHttpObjectAggregator. max-content-length))

        (.addLast "streamer" (ChunkedWriteHandler.))
        #_ (.addLast "ws-compr" (WebSocketServerCompressionHandler.)) ; needs allowExtensions below
        (.addLast "ws" (WebSocketServerProtocolHandler.
                         ; TODO [application] could specify subprotocol?
                         ; https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API/Writing_WebSocket_servers#subprotocols
                         ws-path nil true max-frame-size handshake-timeout))

        ; TODO replace with functionality in main handler
        #_(.addLast "ws-agg" (WebSocketFrameAggregator. max-message-size))
        ; FIXME debug design/ref counting
        #_(.addLast "ws-disk-agg" (DiskWebSocketFrameAggregator. max-message-size))

        ; These handlers are functions returning proxy or reify, i.e. new instance per channel:
        ; (See `ChannelHandler` doc regarding state.)
        ;(.addLast "ws-handler" (ws/handler (assoc admin :type :ws)))
        ;(.addLast "http-handler" (http/handler (assoc admin :type :http)))))))
        (.addLast "handler" (aggregate-and-handle opts))))))