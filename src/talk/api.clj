(ns talk.api
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! take! put! close!]]
            [talk.server :as server]
            [talk.http :as http]
            [talk.ws :as ws]
            [talk.util :refer [retag]]
            [hato.websocket :as hws]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen])
  (:import (io.netty.bootstrap ServerBootstrap)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.group DefaultChannelGroup)
           (io.netty.channel.socket.nio NioServerSocketChannel)
           (io.netty.util.concurrent GlobalEventExecutor)
           (java.net InetSocketAddress)
           (io.netty.handler.codec.http.multipart DefaultHttpDataFactory)
           (io.netty.handler.codec.http HttpObjectDecoder)
           (io.netty.channel ChannelFutureListener DefaultChannelId)
           (java.nio ByteBuffer)
           (java.io ByteArrayOutputStream)
           (java.nio.channels Channels WritableByteChannel))
  (:refer-clojure :exclude [deliver]))

(s/def ::incoming (s/multi-spec server/message-type retag))

(s/def ::outgoing (s/or ::http/response ::http/response
                        ::ws/Text ::ws/Text
                        ::ws/Binary ::ws/Binary))

#_ (s/exercise ::http/Request)
#_ (s/exercise ::outgoing)
#_ (s/exercise ::incoming) ; FIXME have another look at retag and multi-spec (sub-specs work)

; TODO: (see user.clj)
; Give error if client tries to connect ws at wrong path
; Routing entirely within application (bidi I guess)
; HTTP basics - some in application; could plagiarise bits of Ring
; spec all messages
; vigorous benchmarking and stress testing
; Do set up static file serving for convenience? Maybe just individual files?

(def defaults
  "Starts as `opts` and eventually becomes `channel-opts`.
   A state map atom `:state` is added in channel-specific initialiser's initChannel.
   State will include a reference to Netty's ChannelHandlerContext `:ctx`, added in channel-specific handler's channelActive."
  ; TODO spec opts (and follow through!) probably need real config system
  {; Toplevel
   :ws-path "/ws" :in-buffer 1 :out-buffer 1 :handler-timeout (* 5 1000)
   ; Aggregation
   :disk-threshold DefaultHttpDataFactory/MINSIZE
   ; WebSocket
   :handshake-timeout (* 5 1000) ; netty default not public
   :max-frame-size (* 64 1024)
   :max-message-size (* 1024 1024)
   ; HTTP
   :max-chunk-size HttpObjectDecoder/DEFAULT_MAX_CHUNK_SIZE
   :max-content-length (* 1 1024 1024)}) ; limited to int range by netty!

(defn server!
  "Bootstrap a Netty server connected to core.async channels:
    `in` - from which application takes incoming messages (could pub with reference to @clients :type)
    `out` - to which application puts outgoing messages
   Client connections and disconnections appear on `in`.
   Clients are tracked in `clients` atom which contains a map of ChannelId -> arbitrary metadata map.
   Clients can be individually evicted (i.e. have their channel closed) using `evict` fn.
   Close server by calling `close`.
   Websocket path defaults to /ws. Doesn't support Server Sent Events or long polling at present."
  ([port] (server! port {}))
  ([port opts]
   (log/debug "Starting server with" opts)
   (let [{:keys [ws-path in-buffer out-buffer] :as opts} (merge defaults opts)
         ; TODO look at aleph for epoll, thread number specification
         loop-group (NioEventLoopGroup.)
         ; single threaded executor is for group actions
         channel-group (DefaultChannelGroup. GlobalEventExecutor/INSTANCE)
         ; channel-group tracks channels but is not flexible enough for client metadata
         ; therefore store metadata in parallel atom:
         ; also for administering sub(scription)s
         clients (atom {})
         in (chan in-buffer) ; messages to application from server's handlers
         out (chan out-buffer ; messages from application
               (filter (fn [msg]
                         (if-let [explanation (s/explain-data ::outgoing msg)]
                           ; FIXME not sure this actually gets to the log
                           (log/error "Invalid outgoing message" msg explanation)
                           ; TODO bad ::http/response -> 500 or something (and change filter to map)
                           ;  but bad ::ws/... -> ??
                           msg))))
         out-pub (async/pub out :channel) ; ...to server's handler for that netty channel
         evict (fn [^DefaultChannelId id]
                 (let [ast (.asShortText id)]
                   (log/info "Trying to evict" ast (-> (get @clients id) (dissoc :out-sub)))
                   (some-> channel-group (.find id) .close
                     (.addListener
                       (reify ChannelFutureListener
                         (operationComplete [_ f]
                           (if (.isSuccess f)
                             (log/info "Evicted" ast)
                             (log/warn "Couldn't evict" ast))))))))]
     (try (let [bootstrap (doto (ServerBootstrap.)
                            ; TODO any need for separate parent and child groups?
                            (.group loop-group)
                            (.channel NioServerSocketChannel)
                            ; FIXME specify ip ? InetAddress/getLocalHost ?
                            (.localAddress (InetSocketAddress. port))
                            (.childHandler (server/pipeline ws-path
                                             (assoc opts
                                               :channel-group channel-group
                                               :clients clients
                                               :in in :out out
                                               :out-pub out-pub))))
                                               ; avoid dep cycle
                                               ;:ws-send ws/send!))))
                ; I think sync here causes binding to fail here rather than later
                server-cf (-> bootstrap .bind .sync)]
            (log/debug "Bootstrapped")
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

(defprotocol Joinable
  (append [this to]))
(extend-protocol Joinable
  CharSequence
  (append [this ^StringBuilder to]
    (.append (or to (StringBuilder.)) this))
  ByteBuffer
  (append [this ^ByteArrayOutputStream to]
    (let [baos ^ByteArrayOutputStream (or to (ByteArrayOutputStream.))
          ; Wasteful newChannel for every ByteBuffer but ReadOnlyBufferException trying to use .array
          ; https://stackoverflow.com/a/579616/780743
          ch (Channels/newChannel ^ByteArrayOutputStream baos)]
      (.write ch this)
      baos)))

(defprotocol Deliverable
  (deliver [this]))
(extend-protocol Deliverable
  StringBuilder
  (deliver [this] (.toString this))
  ByteArrayOutputStream
  (deliver [this] (.toByteArray this)))

; TODO move hato dep to dev-only; just have client adaptor
(defn client!
  [uri]
  (let [raw-in (chan)
        in (chan)
        _ (go-loop [agg nil]
            (if-let [[frame last?] (<! raw-in)]
              (let [ret (append frame agg)] ; TODO warn if large, abort if huge?
                (if last?
                  (if (>! in (deliver ret))
                    (recur nil)
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
                 ; and https://www.iana.org/assignments/websocket/websocket.xml
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