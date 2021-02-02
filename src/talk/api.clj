(ns talk.api
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! take! put! close!]]
            [talk.http :as http]
            [talk.ws :as ws]
            [hato.websocket :as hws]
            [clojure.spec.alpha :as s])
  (:import (io.netty.bootstrap ServerBootstrap)
           (io.netty.channel ChannelInitializer)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.group DefaultChannelGroup)
           (io.netty.channel.socket SocketChannel)
           (io.netty.channel.socket.nio NioServerSocketChannel)
           (io.netty.util.concurrent GlobalEventExecutor)
           (java.net InetSocketAddress)
           (io.netty.handler.codec.http HttpServerCodec HttpObjectAggregator
                                        HttpContentCompressor HttpContentDecompressor
                                        HttpContentEncoder HttpContentDecoder)
           (io.netty.handler.codec.http.websocketx WebSocketServerProtocolHandler
                                                   WebSocketFrameAggregator)
           (io.netty.handler.codec.http.websocketx.extensions.compression
             WebSocketServerCompressionHandler)
           (io.netty.handler.stream ChunkedWriteHandler)))

(defn pipeline
  [^String ws-path
   {:keys [^int max-content-length ; max HTTP upload
           ^int handshake-timeout
           ^int max-frame-size
           ^int max-message-size] ; max WS message
    :or {max-content-length (* 1024 1024)
         handshake-timeout (* 5 1000)
         max-frame-size (* 64 1024)
         max-message-size (* 1024 1024)}
    :as opts}
   admin] ; admin is handler-opts merged with other kvs (see caller)
  (proxy [ChannelInitializer] []
    (initChannel [^SocketChannel ch]
      (doto (.pipeline ch)
        ; TODO could add selectively according to need
        (.addLast "http" (HttpServerCodec.))
        ; less network but more memory
        #_(.addLast "http-compr" (HttpContentCompressor.))
        #_(.addLast "http-decompr" (HttpContentDecompressor.))
        ; inbound only https://stackoverflow.com/a/38947978/780743
        ; TODO make own to-disk aggregator? https://github.com/netty/netty/issues/8195
        (.addLast "http-agg" (HttpObjectAggregator. max-content-length))
        (.addLast "streamer" (ChunkedWriteHandler.))
        #_ (.addLast "ws-compr" (WebSocketServerCompressionHandler.)) ; needs allowExtensions below
        (.addLast "ws" (WebSocketServerProtocolHandler.
                         ; TODO [application] could specify subprotocol?
                         ; https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API/Writing_WebSocket_servers#subprotocols
                         ws-path nil true max-frame-size handshake-timeout))
        (.addLast "ws-agg" (WebSocketFrameAggregator. max-message-size))
        (.addLast "ws-handler" (ws/handler (assoc admin :type :ws)))
        (.addLast "http-handler" (http/handler (assoc admin :type :http)))))))

(defn server!
  "Bootstrap a Netty server connected to core.async channels:
    `in` - from which application takes incoming messages (could pub with reference to @clients :type)
    `out` - to which application puts outgoing messages
   Client connections and disconnections appear on `in`.
   Clients are tracked in `clients` atom which contains a map of ChannelId -> arbitrary metadata map.
   Clients can be individually evicted (i.e. have their channel closed) using `evict` fn.
   Websocket path defaults to /ws"
  ([port] (server! port {}))
  ([port {:keys [ws-path in-buffer out-buffer handler-opts]
          :or {ws-path "/ws" in-buffer 1 out-buffer 1}
          :as opts}]
   (let [; TODO look at aleph for epoll, thread number specification
         loop-group (NioEventLoopGroup.)
         ; single threaded executor is for group actions
         channel-group (DefaultChannelGroup. GlobalEventExecutor/INSTANCE)
         ; channel-group tracks channels but is not flexible enough for client metadata
         ; therefore store metadata in parallel atom:
         ; also for administering sub(scription)s
         clients (atom {})
         in (chan in-buffer)
         out (chan out-buffer)
         out-pub (async/pub out :ch)
         evict (fn [id] (some-> channel-group (.find id) .close))]
     (try (let [bootstrap (doto (ServerBootstrap.)
                            ; TODO any need for separate parent and child groups?
                            (.group loop-group)
                            (.channel NioServerSocketChannel)
                            (.localAddress ^int (InetSocketAddress. port))
                            (.childHandler (pipeline ws-path
                                             (dissoc opts :handler-opts)
                                             (merge handler-opts
                                               {:channel-group channel-group
                                                :clients clients
                                                :in in
                                                :out-pub out-pub}))))
                ; I think sync here causes binding to fail here rather than later
                server-cf (-> bootstrap .bind .sync)]
            {:close (fn [] (close! out)
                      (some-> server-cf .channel .close .sync)
                      (-> channel-group .close .sync)
                      (close! in)
                      ; could/should add .sync; makes tests slower to exit
                      (-> loop-group .shutdownGracefully))
             :port port :path ws-path :in in :out out :clients clients :evict evict})
          (catch Exception e
            (close! out)
            (close! in)
            (-> loop-group .shutdownGracefully .sync)
            (log/error "Unable to bootstrap server" e)
            (throw e))))))

; TODO move hato dep to dev-only; just have client adaptor
(defn client!
  [uri]
  (let [raw-in (chan)
        in (chan)
        _ (go-loop [agg ""]
            (if-let [[frame last?] (<! raw-in)]
              (let [ret (str agg frame)] ; TODO warn if large, abort if huge?
                (if last?
                  (if (>! in ret)
                    (recur "")
                    (log/error "Dropped incoming message because in chan is closed"))
                  (recur ret)))
              (log/warn "Aggregator chan is closed")))
        out (chan)
        ws @(hws/websocket uri
              {:on-message
               (fn [ws frame last?]
                 (when-not (>!! raw-in [frame last?])
                   (log/error "Dropped incoming message because aggregator chan is closed" frame)))
               :on-close
               (fn [ws status reason]
                 ; Status codes https://tools.ietf.org/html/rfc6455#section-7.4.1
                 (log/info "Websocket closed" status reason))
               :on-error
               (fn [ws error]
                 (log/error "Websocket error" error))})
        _ (go-loop []
            (if-let [msg (<! out)]
              (do #_(log/debug "about to send" (count msg) "characters from client")
                (hws/send! ws msg)
                (recur))
              (log/info "Stopped sending messages")))]
    {:ws ws :in in :out out}))