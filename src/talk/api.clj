(ns talk.api
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! take! put! close!]]
            [talk.server :as server]
            [talk.http :as http]
            [talk.ws :as ws]
            [talk.util :refer [retag ess wrap-channel-group]]
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
           (io.netty.channel ChannelFutureListener DefaultChannelId Channel ChannelId)
           (java.nio ByteBuffer)
           (java.io ByteArrayOutputStream)
           (java.nio.channels Channels))
  (:refer-clojure :exclude [deliver]))

(s/def ::incoming (s/multi-spec server/message-type retag))

(s/def ::outgoing (s/or ::http/response ::http/response
                        ::ws/Text ::ws/Text
                        ::ws/Binary ::ws/Binary))

#_ (s/exercise ::http/Request)
#_ (s/exercise ::outgoing)
#_ (s/exercise ::incoming) ; FIXME have another look at retag and multi-spec (sub-specs work)

(s/def ::upload-approval? boolean?) ; i.e. does application need to approve uploads
(s/def ::allow-origin string?)
(s/def ::ws-path (s/and string? #(re-matches #"/.*" %))) ; TODO refine
(s/def ::in-buffer pos-int?)
(s/def ::out-buffer pos-int?)
(s/def ::timeout (s/int-in 10 10001))
(s/def ::handler-timeout ::timeout)
(s/def ::disk-threshold (s/int-in 1024 (* 1024 1024)))
(s/def ::handshake-timeout ::timeout)
(s/def ::max-frame-size (s/int-in (* 32 1024) (* 1024 1024)))
(s/def ::max-message-size (s/int-in (* 32 1024) (* 1024 1024 1024))) ; TODO cover with tests....
(s/def ::max-chunk-size (s/int-in 1024 (* 1024 1024)))
(s/def ::max-content-length (s/int-in (* 32 1024) (* 1024 1024 1024))) ; but netty uses signed 32bit int!
(s/def ::opts (s/keys :req-un [::in-buffer ::out-buffer ::handler-timeout
                               ::disk-threshold
                               ::handshake-timeout ::max-frame-size ::max-message-size
                               ::max-chunk-size ::max-content-length]
                :opt-un [::upload-approval? ::allow-origin ::ws-path]))

(def defaults
  "Starts as `opts` and eventually becomes `channel-opts`.
   Need to add :ws-path if want websocket."
  {; Toplevel
   :in-buffer 1 :out-buffer 1 :handler-timeout (* 5 1000) :approve-uploads? true
   ; Aggregation
   :disk-threshold DefaultHttpDataFactory/MINSIZE ; 16KiB
   ; WebSocket
   :handshake-timeout (* 5 1000) ; netty default not public
   :max-frame-size (* 64 1024)
   :max-message-size (* 1024 1024)
   ; HTTP
   :max-chunk-size HttpObjectDecoder/DEFAULT_MAX_CHUNK_SIZE
   :max-content-length (* 1 1024 1024)}) ; limited to int range by netty!

(defn server!
  "Bootstrap a Netty server connected to core.async channels:
    `in` - from which application takes incoming messages
    `out` - to which application puts outgoing messages

   All messages in both directions include the channel id on which they occurred.
   Metadata is stored using Netty's Channel's AttributeMap.
   Client connections and disconnections appear on `in` and are tracked in `clients`.
   Clients can be individually evicted (i.e. have their channel closed) using `evict` fn.
   Close server by calling `close`.

   Specify websocket path with :ws-path opt. No ws if not specified.

   Specify allow-origin value with :allow-origin opt. No CORS if not specified.

   Doesn't support Server Sent Events or long polling at present."
  ([port] (server! port {}))
  ([port opts]
   (log/debug "Starting server with" opts)
   (let [merged-opts (merge defaults opts)
         {:keys [ws-path in-buffer out-buffer] :as opts}
         (if-let [explanation (s/explain-data ::opts merged-opts)]
           (throw (ex-info "Invalid opts" {:defaults defaults :opts opts :explanation explanation}))
           merged-opts)
         ; TODO look at aleph for epoll, thread number specification
         loop-group (NioEventLoopGroup.)
         ; single threaded executor is for group actions
         channel-group (DefaultChannelGroup. GlobalEventExecutor/INSTANCE)
         in (chan in-buffer) ; messages to application from server's handlers
         out (chan 1 ; messages from application
               (filter (fn [msg]
                         (if-let [explanation (s/explain-data ::outgoing msg)]
                           ; FIXME not sure this actually gets to the log
                           (log/error "Invalid outgoing message" msg explanation)
                           ; TODO bad ::http/response -> 500 or something (and change filter to map)
                           ;  but bad ::ws/... -> ??
                           msg))))
         out-pub (async/pub out :channel (fn [topic] (async/buffer out-buffer))) ; ...to handler for that netty channel
         evict (fn [^ChannelId id]
                 (log/info "Trying to evict" (ess id))
                 (some-> channel-group (.find id) .close
                   (.addListener
                     (reify ChannelFutureListener
                       (operationComplete [_ f]
                         (if (.isSuccess f)
                           (log/info "Evicted" (ess id))
                           (log/warn "Couldn't evict" (ess id))))))))]
     (try (let [bootstrap (doto (ServerBootstrap.)
                            ; TODO any need for separate parent and child groups?
                            (.group loop-group)
                            (.channel NioServerSocketChannel)
                            ; FIXME specify ip ? InetAddress/getLocalHost ?
                            (.localAddress (InetSocketAddress. port))
                            (.childHandler (server/pipeline
                                             (assoc opts
                                               :channel-group channel-group
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
             :port port :ws-path ws-path :in in :out out
             :clients (wrap-channel-group channel-group) :evict evict})
          (catch Exception e
            (close! out)
            (close! in)
            (-> loop-group .shutdownGracefully .sync)
            (log/error "Unable to bootstrap server" e)
            (throw e))))))

; Client

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