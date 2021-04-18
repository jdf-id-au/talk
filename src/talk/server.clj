(ns talk.server
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]]
            [talk.http :as http]
            [talk.ws :as ws]
            [talk.util :refer [on]]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen])
  (:import (io.netty.channel ChannelInitializer ChannelHandlerContext ChannelHandler
                             SimpleChannelInboundHandler ChannelOption
                             ChannelId DefaultChannelId)
           (io.netty.channel.socket SocketChannel)
           (io.netty.handler.codec.http HttpServerCodec HttpObjectDecoder
                                        HttpObjectAggregator)
           (io.netty.handler.stream ChunkedWriteHandler)
           (io.netty.handler.codec.http HttpUtil HttpObject)
           (io.netty.handler.codec.http.websocketx WebSocketServerProtocolHandler
                                                   WebSocketFrameAggregator)
           (io.netty.util ReferenceCountUtil)
           (talk.http Connection Request Attribute File Trail)
           (talk.ws Text Binary)
           (io.netty.handler.codec.http.cors CorsHandler CorsConfig CorsConfigBuilder$ConstantValueGenerator CorsConfigBuilder)))

(s/def ::channel (s/with-gen #(instance? ChannelId %)
                   #(gen/fmap (fn [_] (DefaultChannelId/newInstance)) (s/gen nil?))))

(defmulti message-type class)
(defmethod message-type Connection [_] ::http/Connection)
(defmethod message-type Request [_] ::http/Request)
(defmethod message-type Attribute [_] ::http/Attribute)
(defmethod message-type File [_] ::http/File)
(defmethod message-type Trail [_] ::http/Trail)
(defmethod message-type Text [_] ::ws/Text)
(defmethod message-type Binary [_] ::ws/Binary)

(defn ^ChannelHandler tracker
  [{:keys [] :as opts}]
  (proxy [SimpleChannelInboundHandler] [HttpObject]
    (channelActive [^ChannelHandlerContext ctx]
        (http/track-channel opts ctx)
        (-> ctx .channel .config
              ; facilitate backpressure on subsequent reads; requires .read see branches below
          (-> (.setAutoRead false)
              ; May be needed for response from outside netty event loop:
              ; https://stackoverflow.com/a/48128514/780743
              (.setOption ChannelOption/ALLOW_HALF_CLOSURE true)))
        (.read ctx)) ; first read
    (channelRead0 [^ChannelHandlerContext ctx ^HttpObject obj]
      (ReferenceCountUtil/retain obj)
      (.fireChannelRead ctx obj))))

(defn pipeline
  [{:keys [^String ws-path
           ^String allow-origin
           ^int handshake-timeout
           ^int max-frame-size ^int max-message-size
           ^int max-chunk-size ^int max-content-length] :as opts}]
  (log/debug "Starting pipeline")
  (let [^CorsConfigBuilder ccb
        (if allow-origin (doto (CorsConfigBuilder/forOrigin allow-origin)
                               (.allowedRequestHeaders (into-array ["content-type"])))
                         (.disable (CorsConfigBuilder/forAnyOrigin)))]
    (proxy [ChannelInitializer] []
      (initChannel [^SocketChannel ch]
        (doto (.pipeline ch)
          ; TODO could add handlers selectively according to need (from within the channel)
          (.addLast "http" (HttpServerCodec.
                             HttpObjectDecoder/DEFAULT_MAX_INITIAL_LINE_LENGTH
                             HttpObjectDecoder/DEFAULT_MAX_HEADER_SIZE
                             max-chunk-size))
          (.addLast "tracker" (tracker opts))
          ; less network but more memory
          #_(.addLast "http-compr" (HttpContentCompressor.))
          #_(.addLast "http-decompr" (HttpContentDecompressor.))
          (.addLast "streamer" (ChunkedWriteHandler.))
          (.addLast "cors" (CorsHandler. (.build ccb)))
          (.addLast "http-handler" (http/handler opts)))
        (when ws-path
          (doto (.pipeline ch)
            ; Needed for WebSocketServerProtocolHandler but not wanted for http/handler
            ; (so HttpPostRequestDecoder streams properly)
            ; so need to pass through ws upgrade requests...
            (.addLast "http-agg" (HttpObjectAggregator. max-content-length))
            #_(.addLast "ws-compr" (WebSocketServerCompressionHandler.)) ; needs allowExtensions
            (.addLast "ws" (WebSocketServerProtocolHandler.
                             ; TODO [application] could specify subprotocol?
                             ; https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API/Writing_WebSocket_servers#subprotocols
                             ws-path nil true max-frame-size handshake-timeout))
            ; TODO replace with functionality in main handler (e.g. via disk if big)
            (.addLast "ws-agg" (WebSocketFrameAggregator. max-message-size))
            (.addLast "ws-handler" (ws/handler opts))))))))
