(ns talk.server
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]]
            [talk.http :as http]
            [talk.ws :as ws])
  (:import (io.netty.channel ChannelInitializer)
           (io.netty.channel.socket SocketChannel)
           (io.netty.handler.codec.http HttpServerCodec HttpObjectDecoder HttpObjectAggregator)
           (io.netty.handler.stream ChunkedWriteHandler)
           (io.netty.handler.codec.http.websocketx WebSocketServerProtocolHandler
                                                   WebSocketFrameAggregator)))

(defn pipeline
  [^String ws-path
   {:keys [^int handshake-timeout
           ^int max-frame-size ^int max-message-size
           ^int max-chunk-size ^int max-content-length] :as opts}]
  (log/debug "Starting pipeline")
  (proxy [ChannelInitializer] []
    (initChannel [^SocketChannel ch]
      ; add state atom instead of using netty's Channel.attr
      (let [channel-opts (assoc opts :state (atom {}))]
        (doto (.pipeline ch)
          ; TODO could add handlers selectively according to need (from within the channel)
          (.addLast "http" (HttpServerCodec.
                             HttpObjectDecoder/DEFAULT_MAX_INITIAL_LINE_LENGTH
                             HttpObjectDecoder/DEFAULT_MAX_HEADER_SIZE
                             max-chunk-size))
          ; less network but more memory
          #_(.addLast "http-compr" (HttpContentCompressor.))
          #_(.addLast "http-decompr" (HttpContentDecompressor.))
          (.addLast "streamer" (ChunkedWriteHandler.))
          (.addLast "http-handler" (http/handler channel-opts))
          ; Only needed to make WebSocketServerProtocolHandler work!
          ; http/handler should be before this so HttpPostRequestDecoder streams properly
          (.addLast "http-agg" (HttpObjectAggregator. max-content-length))
          #_(.addLast "ws-compr" (WebSocketServerCompressionHandler.)) ; needs allowExtensions
          (.addLast "ws" (WebSocketServerProtocolHandler.
                           ; TODO [application] could specify subprotocol?
                           ; https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API/Writing_WebSocket_servers#subprotocols
                           ws-path nil true max-frame-size handshake-timeout))
          ; TODO replace with functionality in main handler
          (.addLast "ws-agg" (WebSocketFrameAggregator. max-message-size))
          ; These handlers are functions returning proxy or reify, i.e. new instance per channel:
          ; (See `ChannelHandler` doc regarding state.)
          (.addLast "ws-handler" (ws/handler channel-opts)))))))
