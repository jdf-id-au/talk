(ns talk.common
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! put! close!]])
  (:import (io.netty.channel ChannelHandlerContext ChannelFutureListener)
           (io.netty.channel.group ChannelGroup)
           (java.net InetSocketAddress)))

(defn track-channel
  [^ChannelHandlerContext ctx
   {:keys [^ChannelGroup channel-group
           clients in out-pub type]
    :as admin}]
  (let [ch (.channel ctx)
        id (.id ch)
        cf (.closeFuture ch)
        out-sub (chan)]
    (try (.add channel-group ch)
         (async/sub out-pub id out-sub)
         (swap! clients assoc id
           {:type type
            :out-sub out-sub
            :addr (-> ch ^InetSocketAddress .remoteAddress .getAddress .toString)})
         (when-not (put! in {:ch id :type type :connected true})
          (log/error "Unable to report connection because in chan is closed"))
         (.addListener cf
           (reify ChannelFutureListener
             (operationComplete [_ _]
               (swap! clients dissoc id)
               (when-not (put! in {:ch id :connected false})
                 (log/error "Unable to report disconnection because in chan is closed")))))
         (catch Exception e
           (log/error "Unable to register channel" ch e)
           (throw e)))
    #_(.fireChannelActive ctx)))