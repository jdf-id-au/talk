(ns talk.api
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! take! put! close!]]
            [talk.server :as server]
            [talk.http :as http]
            [talk.ws :as ws]
            [hato.websocket :as hws]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen])
  (:import (io.netty.bootstrap ServerBootstrap)
           (io.netty.channel ChannelId DefaultChannelId)
           (io.netty.channel.nio NioEventLoopGroup)
           (io.netty.channel.group DefaultChannelGroup)
           (io.netty.channel.socket.nio NioServerSocketChannel)
           (io.netty.util.concurrent GlobalEventExecutor)
           (java.net InetSocketAddress)))

(defn retag [gen-v tag] gen-v) ; copied from jdf/comfort for the moment

(s/def ::ch (s/with-gen #(instance? ChannelId %)
              #(gen/fmap (fn [_] (DefaultChannelId/newInstance)) (s/gen nil?))))
(s/def ::connected boolean?)
(s/def ::connection (s/keys [:req-un [::ch ::connected]]))

(defmulti message-type :type)
(defmethod message-type ::connection [_] ::connection) ; spec actually in talk.common
(defmethod message-type ::http/request [_] ::http/request)
(defmethod message-type ::ws/text [_] ::ws/text)
(defmethod message-type ::ws/binary [_] ::ws/binary)
; :type simplifies dispatch of incoming
(s/def ::incoming (s/multi-spec message-type retag))

; :type not necessary for outgoing
(s/def ::outgoing (s/or ::http/response ::http/response
                        ::ws/text ::ws/text
                        ::ws/binary ::ws/binary))

#_ (s/exercise ::http/request) ; can't gen ::incoming, maybe because :type not specced
#_ (s/exercise ::outgoing)

(defn server!
  "Bootstrap a Netty server connected to core.async channels:
    `in` - from which application takes incoming messages (could pub with reference to @clients :type)
    `out` - to which application puts outgoing messages
   Client connections and disconnections appear on `in`.
   Clients are tracked in `clients` atom which contains a map of ChannelId -> arbitrary metadata map.
   Clients can be individually evicted (i.e. have their channel closed) using `evict` fn.
   Close server by calling `close`.
   Websocket path defaults to /ws. Doesn't support Server Sent Events or long polling at present."
  ; TODO adjust messages to be suitable for spec conformation (see above)
  ([port] (server! port {}))
  ([port {:keys [ws-path in-buffer out-buffer]
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
         in (chan in-buffer) ; messages to application from server's handlers
         out (chan out-buffer) ; messages from application
         out-pub (async/pub out :ch) ; ...to server's handler for that netty channel
         evict (fn [id] (some-> channel-group (.find id) .close))]
     (try (let [bootstrap (doto (ServerBootstrap.)
                            ; TODO any need for separate parent and child groups?
                            (.group loop-group)
                            (.channel NioServerSocketChannel)
                            (.localAddress ^int (InetSocketAddress. port))
                            (.childHandler (server/pipeline ws-path
                                             (merge opts
                                               {:channel-group channel-group
                                                :clients clients
                                                :in in :out-pub out-pub}))))
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