(ns talk.api
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! take! put! close!]]
            [talk.http :as http]
            [talk.ws :as ws]
            [hato.websocket :as hws]
            [clojure.spec.alpha :as s])
  (:import (io.netty.bootstrap ServerBootstrap)
           (io.netty.channel ChannelInitializer
                             ChannelId ChannelFutureListener)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.group ChannelGroup DefaultChannelGroup)
           (io.netty.channel.socket SocketChannel)
           (io.netty.channel.socket.nio NioServerSocketChannel)
           (io.netty.util.concurrent GlobalEventExecutor)
           (java.net InetSocketAddress)
           (io.netty.handler.codec.http HttpServerCodec HttpObjectAggregator)
           (io.netty.handler.codec.http.websocketx WebSocketServerProtocolHandler
                                                   WebSocketFrameAggregator
                                                   TextWebSocketFrame)
           (io.netty.handler.codec.http.websocketx.extensions.compression
             WebSocketServerCompressionHandler)))

(s/def ::port (s/int-in 1024 65535))
(s/def ::max-frame-size (s/int-in 1024 (* 1024 1024)))
(s/def ::max-message-size (s/int-in 1024 (* 10 1024 1024)))
(s/def ::buffer (s/int-in 1 1024))
(s/def ::in-buffer ::buffer)
(s/def ::out-buffer ::buffer)
(s/def ::timeout (s/int-in 100 (* 10 1000)))
(s/def ::opts (s/keys :opt-un [::max-frame-size
                               ::max-message-size
                               ::in-buffer
                               ::out-buffer]))

(defn pipeline
  [^String ws-path {:keys [^int max-frame-size max-message-size]
                    :or {max-frame-size (* 64 1024)
                         max-message-size (* 1024 1024)}
                    :as opts}
   ^ChannelGroup channel-group clients in]
  (proxy [ChannelInitializer] []
    (initChannel [^SocketChannel ch]
      (doto (.pipeline ch)
        ; TODO could add selectively according to need
        (.addLast "http" (HttpServerCodec.))
        (.addLast "http-agg" (HttpObjectAggregator. (* 64 1024)))
        (.addLast "ws-compr" (WebSocketServerCompressionHandler.)) ; needs allowExtensions below
        (.addLast "ws" (WebSocketServerProtocolHandler.
                         ws-path nil true max-frame-size 10000)) ; compiler can't find static field??:
                         ;WebSocketServerProtocolConfig/DEFAULT_HANDSHAKE_TIMEOUT_MILLIS))
        (.addLast "ws-agg" (WebSocketFrameAggregator. max-message-size))
        (.addLast "http-handler" (http/handler nil)) ; TODO pass through routing info
        (.addLast "ws-handler" (ws/handler channel-group clients in))))))
        ; TODO per message deflate?
        ; HttpContentEncoder HttpContentDecoder
        ; HttpContentCompressor HttpContentDecompressor

(defn server!
  "Bootstrap a Netty server connected to core.async channels:
    `in` - from which application takes incoming messages in the form [ChannelId text]
    `out` - to which application puts outgoing messages in the form [ChannelId text]
   Client connections and disconnections appear on `in` as [ChannelId bool].
   Websocket clients are tracked in `clients` atom which contains a map of ChannelId -> arbitrary metadata map.
   Websocket clients can be individually evicted (i.e. have their channel closed) using `evict` fn."
  ([port] (server! port "/" {}))
  ([port path {:keys [in-buffer out-buffer timeout]
               :or {in-buffer 1 out-buffer 1 timeout 3000}
               :as opts}]
   {:pre [(s/valid? ::port port)
          (s/valid? ::opts opts)]}
   (let [; TODO look at aleph for epoll, thread number specification
         loop-group (NioEventLoopGroup.)
         ; single threaded executor is for group actions
         channel-group (DefaultChannelGroup. GlobalEventExecutor/INSTANCE)
         ; channel-group tracks channels but is not flexible enough for client metadata
         ; therefore store metadata in parallel atom:
         clients (atom {})
         in (chan in-buffer)
         out (chan out-buffer)
         ;pub (async/pub in )
         ; Want outgoing messages to queue up in `out` chan rather than Netty,
         ; so use ChannelFutureListener to drain `out` with backpressure:
         post (fn post [val]
                (if-let [[^ChannelId id ^String msg] val] ; TODO validate
                  (if-let [ch (some->> id (.find channel-group))]
                    #_(log/debug "about to write" (count msg) "characters to"
                        (.remoteAddress ch) "on channel id" (.id ch))
                    (let [cf (.writeAndFlush ch (TextWebSocketFrame. msg))]
                      (.addListener cf
                        (reify ChannelFutureListener
                          (operationComplete [_ f]
                            (when (.isCancelled f)
                              (log/info "Cancelled message" msg "to" id))
                            (when-not (.isSuccess f)
                              (log/error "Send error for " msg "to" id
                                (.cause f)))
                            (take! out post)))))
                    (log/info "Dropped outgoing message because websocket is closed"
                      id (get @clients id) msg))
                  (log/info "Stopped sending messages")))
         _ (take! out post)
         evict (fn [id] (some-> channel-group (.find id) .close))]
     (try (let [bootstrap (doto (ServerBootstrap.)
                            ; TODO any need for separate parent and child groups?
                            (.group loop-group)
                            (.channel NioServerSocketChannel)
                            (.localAddress ^int (InetSocketAddress. port))
                            (.childHandler (pipeline path opts channel-group clients in)))
                server-cf (-> bootstrap .bind .sync)] ; I think sync here causes binding to fail here rather than later
            {:close (fn [] (close! out)
                      (some-> server-cf .channel .close .sync)
                      (-> channel-group .close .sync)
                      (close! in)
                      (-> loop-group .shutdownGracefully)) ; could/should add .sync; makes tests slower
             :port port :path path :in in :out out :clients clients :evict evict})
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