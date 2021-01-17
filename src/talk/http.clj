(ns talk.http
  (:require [talk.common :as common]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! put! close! alt!!]])
  (:import (io.netty.buffer Unpooled ByteBufUtil ByteBuf)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler
                             ChannelHandlerContext ChannelFutureListener)
           (io.netty.handler.codec.http HttpRequest HttpContent HttpUtil
                                        DefaultFullHttpResponse
                                        HttpResponseStatus HttpVersion
                                        FullHttpRequest FullHttpResponse
                                        HttpMethod HttpHeaderNames QueryStringDecoder)
           (io.netty.util CharsetUtil)
           (io.netty.handler.codec.http.cookie ServerCookieDecoder ServerCookieEncoder)
           (java.net InetSocketAddress)
           (io.netty.channel.group ChannelGroup)))

; after https://netty.io/4.1/xref/io/netty/example/http/websocketx/server/WebSocketIndexPageHandler.html
(defn respond! [^ChannelHandlerContext ctx ^FullHttpRequest req ^FullHttpResponse res]
  (let [status (.status res)
        ok? (= status HttpResponseStatus/OK)
        keep-alive? (and (HttpUtil/isKeepAlive req) ok?)]
    (HttpUtil/setKeepAlive res keep-alive?)
    (HttpUtil/setContentLength res (-> res .content .readableBytes))
    (let [cf (.writeAndFlush ctx res)]
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))

(defn ^ChannelHandler handler
  [{:keys [clients in type] :as admin}]
  (proxy [SimpleChannelInboundHandler] [FullHttpRequest]
    ; TODO work through https://github.com/netty/netty/blob/e5951d46fc89db507ba7d2968d2ede26378f0b04/example/src/main/java/io/netty/example/http/snoop/HttpSnoopServerHandler.java
    (channelActive [^ChannelHandlerContext ctx] (common/track-channel ctx admin))
    (channelRead0 [^ChannelHandlerContext ctx ^FullHttpRequest req]
      ; facilitate backpressure on subsequent reads; requires .read see branches below
      #_(-> ctx .channel .config (.setAutoRead false))
      #_(common/track-channel ctx admin nil)
      (respond! ctx req
        (if (-> req .decoderResult .isSuccess)
          (let [ch (.channel ctx)
                id (.id ch)
                out-sub (get-in @clients [id :out-sub])
                qsd (-> req .uri QueryStringDecoder.)]
            (if (put! in
                  {:ch id
                   :method (-> req .method .toString keyword)
                   :path (.path qsd)
                   :query (.parameters qsd)
                   :protocol (-> req .protocolVersion .toString)
                   :headers (->> req .headers .iteratorAsString iterator-seq
                              ; TODO drop cookies headers; are repeated headers coalesced?
                              ; https://stackoverflow.com/questions/4371328/are-duplicate-http-response-headers-acceptable#:~:text=Yes&text=So%2C%20multiple%20headers%20with%20the,comma%2Dseparated%20list%20of%20values.
                              (into {} (map (fn [[k v]] [(keyword k) v]))))
                   :cookies (some->>
                              (some-> req .headers (.get HttpHeaderNames/COOKIE))
                              (.decode ServerCookieDecoder/STRICT)
                              (into {} (map (fn [c] [(.name c) (.value c)]))))
                   :content (some-> req .content (.toString CharsetUtil/UTF_8))}
                  (fn [val]
                    (if val
                      nil #_(.read ctx) ; because autoRead is false
                      (log/error "Dropped incoming http request because in chan is closed"))))
              (let [; NB/FIXME this is blocking
                    {:keys [status headers cookies content]}
                    (alt!! (async/timeout 5000) {:status (.code HttpResponseStatus/SERVICE_UNAVAILABLE)}
                           out-sub ([v] v))
                    _ (log/info "Got from alt!!")
                    buf (condp #(%1 %2) content
                          string? (Unpooled/copiedBuffer ^String content CharsetUtil/UTF_8)
                          nil? Unpooled/EMPTY_BUFFER
                          bytes? (Unpooled/copiedBuffer ^bytes content))
                    res (DefaultFullHttpResponse.
                          (.protocolVersion req)
                          (HttpResponseStatus/valueOf status)
                          ^ByteBuf buf)
                    hdrs (.headers res)]
                (doseq [[k v] headers]
                  (.set hdrs (name k) v))
                ; TODO need to support repeated headers (other than Set-Cookie ?)
                ; TODO trailing headers? for chunked responses?
                (.set hdrs HttpHeaderNames/SET_COOKIE
                  ; TODO expiry?
                  ^Iterable (into [] (for [[k v] cookies]
                                       (.encode ServerCookieEncoder/STRICT k v))))
                res)
              (do (log/error "Dropped incoming http request because in chan is closed")
                  (DefaultFullHttpResponse.
                    (.protocolVersion req)
                    HttpResponseStatus/INTERNAL_SERVER_ERROR
                    Unpooled/EMPTY_BUFFER))))
          (do (log/info "Decoder failure" (-> req .decoderResult .cause))
              (DefaultFullHttpResponse.
                (.protocolVersion req)
                HttpResponseStatus/BAD_REQUEST
                Unpooled/EMPTY_BUFFER)))))
          ;
          ;(-> req .method (= HttpMethod/GET) not)
          ;(respond! ctx req (DefaultFullHttpResponse. (.protocolVersion req)
          ;                    HttpResponseStatus/FORBIDDEN Unpooled/EMPTY_BUFFER))
          ;
          ;(-> req .uri (= "/hmm"))
          ;(let [content (Unpooled/copiedBuffer "hello" CharsetUtil/UTF_8)
          ;      res (DefaultFullHttpResponse. (.protocolVersion req)
          ;            HttpResponseStatus/OK content)
          ;      _ (-> res .headers (.set HttpHeaderNames/CONTENT_TYPE "text/html; charset=UTF-8"))
          ;      _ (HttpUtil/setContentLength res (.readableBytes content))]
          ;  (respond! ctx req res))
          ;
          ;:else
          ;(respond! ctx req (DefaultFullHttpResponse. (.protocolVersion req)
          ;                    HttpResponseStatus/NOT_FOUND Unpooled/EMPTY_BUFFER))))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
      (log/error "Error in http handler" cause)
      (.close ctx))))