(ns talk.server
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]]
            [talk.http :as http]
            [talk.ws :as ws]
            [talk.util :refer [on]]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen])
  (:import (io.netty.channel ChannelInitializer ChannelHandlerContext
                             ChannelFutureListener ChannelHandler
                             SimpleChannelInboundHandler ChannelOption
                             ChannelId DefaultChannelId)
           (io.netty.channel.socket SocketChannel)
           (io.netty.handler.codec.http HttpServerCodec HttpObjectDecoder
                                        HttpObjectAggregator)
           (io.netty.handler.stream ChunkedWriteHandler)
           (io.netty.handler.codec.http HttpUtil HttpObject)
           (io.netty.handler.codec.http.websocketx WebSocketServerProtocolHandler
                                                   WebSocketFrameAggregator)
           (io.netty.channel.group ChannelGroup)
           (java.net InetSocketAddress)
           (io.netty.util ReferenceCountUtil)
           (talk.http Request Attribute File Trail)))

(defrecord Connection [channel open?]
  Object (toString [r] (str "Channel " (on r) \  (if open? "opened" "closed"))))

(s/def ::open? boolean?)
(s/def ::Connection (s/keys :req-un [::ch ::open?]))

(s/def ::ch (s/with-gen #(instance? ChannelId %)
              #(gen/fmap (fn [_] (DefaultChannelId/newInstance)) (s/gen nil?))))

(defmulti message-type class)
(defmethod message-type Connection [_] ::Connection)
(defmethod message-type Request [_] ::http/request)
(defmethod message-type Attribute [_] ::http/Attribute)
(defmethod message-type File [_] ::http/File)
(defmethod message-type Trail [_] ::http/Trail)


(defn track-channel
  "Register channel in `clients` map and report on `in` chan.
   Map entry is a map containing `type`, `out-sub` and `addr`, and can be updated.

   Usage:
   - Call from channelActive.
   - Detect websocket upgrade handshake, using userEventTriggered, and update `clients` map."
  [{:keys [^ChannelGroup channel-group
           state clients in out-pub]}]
  (let [ctx ^ChannelHandlerContext (:ctx @state)
        ch (.channel ctx)
        id (.id ch)
        cf (.closeFuture ch)
        out-sub (chan)]
    (try (.add channel-group ch)
         (async/sub out-pub id out-sub)
         (swap! clients assoc id
           {:type :http ; changed in userEventTriggered
            :out-sub out-sub
            :addr (-> ch ^InetSocketAddress .remoteAddress HttpUtil/formatHostnameForHttp)})
         (when-not (put! in (->Connection id true))
           (log/error "Unable to report connection because in chan is closed"))
         (.addListener cf
           (reify ChannelFutureListener
             (operationComplete [_ _]
               (swap! clients dissoc id)
               (when-not (put! in (->Connection id false))
                 (log/error "Unable to report disconnection because in chan is closed")))))
         (catch Exception e
           (log/error "Unable to register channel" ch e)
           (throw e)))))

(defn ^ChannelHandler tracker
  [{:keys [state] :as opts}]
  (proxy [SimpleChannelInboundHandler] [HttpObject]
    (channelActive [^ChannelHandlerContext ctx]
        ; TODO is this safe?
        ; Doesn't a given channel keep the same context instance,
        ; which is set by the pipeline in initChannel?
        (swap! state assoc :ctx ctx)
        (track-channel opts)
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
          (.addLast "tracker" (tracker channel-opts))
          ; less network but more memory
          #_(.addLast "http-compr" (HttpContentCompressor.))
          #_(.addLast "http-decompr" (HttpContentDecompressor.))
          (.addLast "streamer" (ChunkedWriteHandler.))
          (.addLast "http-handler" (http/handler channel-opts))
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
          (.addLast "ws-handler" (ws/handler channel-opts)))))))
