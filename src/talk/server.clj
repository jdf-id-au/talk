(ns talk.server
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]]
            [talk.http :as http]
            [talk.ws :as ws])
  (:import (io.netty.channel ChannelInitializer ChannelHandlerContext
                             ChannelInboundHandler ChannelFutureListener ChannelOption)
           (io.netty.channel.socket SocketChannel)
           (io.netty.handler.codec.http HttpServerCodec)
           (io.netty.handler.stream ChunkedWriteHandler)
           (io.netty.handler.codec.http.websocketx
             WebSocketServerProtocolHandler WebSocketServerProtocolHandler$HandshakeComplete)
           (io.netty.util ReferenceCountUtil)
           (io.netty.channel.group ChannelGroup)
           (java.net InetSocketAddress)
           (java.nio.file Path Files StandardOpenOption)
           (java.nio.channels SeekableByteChannel)))

#_(defprotocol ChannelInboundMessageHandler
    "Dispatch incoming netty msg types.
   Naming convention adapted from `SimpleChannelInboundHandler`."
    (channelRead0 [msg broader-context]
      "Handle specific netty message type.")
    (offer [msg so-far broader-context]
      "Ask for processed message to be aggregated into so-far.
     Return [status result broader-context]."))

#_(defprotocol Aggregator
    (accept [so-far msg broader-context] "Attempt to aggregate processed msg into so-far."))

#_(defn tempfile
    "Return a new tempfile path and its open SeekableByteChannel."
    ; ClosedByInterruptException risk?? https://stackoverflow.com/a/42409658/780743
    [suffix]
    (let [path (Files/createTempFile "talk" suffix [])
          ch (Files/newByteChannel path [StandardOpenOption/CREATE_NEW StandardOpenOption/WRITE])]
      [path ch]))

#_(defrecord Disk [meta ^Path path ^SeekableByteChannel channel]
    Aggregator
    (accept [so-far msg bc]))

#_(defrecord Memory [meta ^bytes content]
    Aggregator
    (accept [so-far msg bc]))

#_(defn aggregator
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

#_(defn ^ChannelInboundHandler aggregate-and-handle
    "Track netty channels and clients.
   Pass netty messages to handlers.
   Also pass a chan which can be used for aggregation of messages on each netty channel."
    [{:keys [clients in ws-send] :as opts}]
    (let [chunks (chan)
          _ (aggregator chunks in) ; TODO make use of return channel value?
          opts (assoc opts :chunks chunks)]
      (reify ChannelInboundHandler ; TODO return hint ^nil, ^void or nothing?
        (channelRegistered [_ ^ChannelHandlerContext ctx] (.fireChannelRegistered ctx))
        (channelUnregistered [_ ^ChannelHandlerContext ctx] (.fireChannelUnregistered ctx))
        (channelActive [_ ^ChannelHandlerContext ctx]
          (-> ctx .channel .config
                ; Facilitate backpressure on subsequent reads (both http and after ws upgrade).
                ; Requires manual `(.read ctx)` to indicate readiness.
            (-> (.setAutoRead false)
                ; May be needed for response from outside netty event loop:
                ; https://stackoverflow.com/a/48128514/780743
                (.setOption ChannelOption/ALLOW_HALF_CLOSURE true)))
          (track-channel ctx opts)
          (.read ctx))
        (channelInactive [_ ^ChannelHandlerContext ctx] (.fireChannelInactive ctx))
        (channelRead [_ ^ChannelHandlerContext ctx msg]
          ; Adapted from SimpleChannelInboundHandler
          #_(let [release? (volatile! true)]
              (try (when-not (channelRead0 msg ctx)
                     (vreset! release? false)
                     (.fireChannelRead ctx msg))
                   (finally (when @release? (ReferenceCountUtil/release msg)))))
          (try (channelRead0 msg (assoc opts :ctx ctx :state (atom {}))) ; fixme should be in channelActive
            ; channelRead0 needs to release msg when done! (done below if Exception)
            (catch Exception e (ReferenceCountUtil/release msg) (throw e)))
          nil)
        (channelReadComplete [_ ^ChannelHandlerContext ctx] (.fireChannelReadComplete ctx))
        (userEventTriggered [_ ^ChannelHandlerContext ctx evt]
          (if (instance? WebSocketServerProtocolHandler$HandshakeComplete evt)
            (let [ch (.channel ctx) id (.id ch)
                  out-sub (get-in @clients [id :out-sub])]
              (swap! clients update id assoc :type :ws)
              ; first take!, see ws/send! for subsequent
              (async/take! out-sub (partial ws-send ctx out-sub)))
            (.fireUserEventTriggered ctx evt)))
        (channelWritabilityChanged [_ ^ChannelHandlerContext ctx]
          (.fireChannelWritabilityChanged ctx))
        (exceptionCaught [_ ^ChannelHandlerContext ctx ^Throwable cause]
          (.fireExceptionCaught ctx cause)))))
      ; ChannelHandler methods
      ;(handlerAdded [_ ^ChannelHandlerContext ctx])
      ;(handlerRemoved [_ ^ChannelHandlerContext ctx])))

(defn pipeline
  [^String ws-path
   {:keys [^int handshake-timeout ^int max-frame-size] :as opts}]
  (proxy [ChannelInitializer] []
    (initChannel [^SocketChannel ch]
      ; add state atom instead of using netty's Channel.attr
      (let [channel-opts (assoc opts :state (atom {}))]
        (doto (.pipeline ch)
          ; TODO could add handlers selectively according to need (from within the channel)
          (.addLast "http" (HttpServerCodec.)) ; TODO could tune chunk size
          ; less network but more memory
          #_(.addLast "http-compr" (HttpContentCompressor.))
          #_(.addLast "http-decompr" (HttpContentDecompressor.))

          ; inbound only https://stackoverflow.com/a/38947978/780743
          ; TODO replace with functionality in main handler
          #_(.addLast "http-agg" (HttpObjectAggregator. max-content-length))

          (.addLast "streamer" (ChunkedWriteHandler.))
          #_ (.addLast "ws-compr" (WebSocketServerCompressionHandler.)) ; needs allowExtensions
          (.addLast "ws" (WebSocketServerProtocolHandler.
                           ; TODO [application] could specify subprotocol?
                           ; https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API/Writing_WebSocket_servers#subprotocols
                           ws-path nil true max-frame-size handshake-timeout))

          ; TODO replace with functionality in main handler
          #_(.addLast "ws-agg" (WebSocketFrameAggregator. max-message-size))
          ; These handlers are functions returning proxy or reify, i.e. new instance per channel:
          ; (See `ChannelHandler` doc regarding state.)
          (.addLast "ws-handler" (ws/handler channel-opts))
          (.addLast "http-handler" (http/handler channel-opts))
          #_(.addLast "handler" (aggregate-and-handle opts)))))))