(ns talk.http
  (:require [talk.common :as common]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go-loop chan <!! >!! <! >! put! close! alt!!]])
  (:import (io.netty.buffer Unpooled ByteBuf)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler
                             ChannelHandlerContext ChannelFutureListener)
           (io.netty.handler.codec.http HttpUtil
                                        DefaultFullHttpResponse
                                        HttpResponseStatus
                                        FullHttpRequest FullHttpResponse
                                        HttpHeaderNames QueryStringDecoder HttpMethod)
           (io.netty.util CharsetUtil)
           (io.netty.handler.codec.http.cookie ServerCookieDecoder ServerCookieEncoder)
           (io.netty.handler.codec.http.multipart HttpPostRequestDecoder Attribute InterfaceHttpData FileUpload)))

; after https://netty.io/4.1/xref/io/netty/example/http/websocketx/server/WebSocketIndexPageHandler.html
(defn respond!
  "Send HTTP response, and manage keep-alive and Content-Length."
  [^ChannelHandlerContext ctx ^FullHttpRequest req ^FullHttpResponse res]
  (let [status (.status res)
        ok? (= status HttpResponseStatus/OK)
        keep-alive? (and (HttpUtil/isKeepAlive req) ok?)]
    (HttpUtil/setKeepAlive res keep-alive?)
    ; May need to review when enabling HttpContentEncoder etc. What about HTTP/2?
    (HttpUtil/setContentLength res (-> res .content .readableBytes))
    (let [cf (.writeAndFlush ctx res)]
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))
      ; No need for backpressure here? (c.f. ws/send!)

(defn ^ChannelHandler handler
  "Parse HTTP requests and forward to `in` with backpressure. Respond *synchronously* from `out-sub`."
  ; TODO read about HTTP/2 https://developers.google.com/web/fundamentals/performance/http2
  [{:keys [clients in] :as admin}]
  (proxy [SimpleChannelInboundHandler] [FullHttpRequest]
    (channelActive [^ChannelHandlerContext ctx] (common/track-channel ctx admin))
    (channelRead0 [^ChannelHandlerContext ctx ^FullHttpRequest req]
      ; facilitate backpressure on subsequent reads; requires .read see branches below
      (-> ctx .channel .config (.setAutoRead false))
      ; TODO NB `respond!` in channelRead0 is inherently synchronous... check whether this is the right way... can't find counterexample...
      (respond! ctx req
        (if (-> req .decoderResult .isSuccess)
          (let [ch (.channel ctx)
                id (.id ch)
                out-sub (get-in @clients [id :out-sub])
                method (.method req)
                qsd (-> req .uri QueryStringDecoder.)]
            ; TODO reorganise if gets too long
            (if (put! in
                  (cond->
                    {:ch id
                     :method (-> method .toString keyword)
                     :path (.path qsd)
                     :query (.parameters qsd)
                     :protocol (-> req .protocolVersion .toString)
                     ; TODO drop cookies headers; are repeated headers coalesced?
                     ; https://stackoverflow.com/questions/4371328/are-duplicate-http-response-headers-acceptable#:~:text=Yes&text=So%2C%20multiple%20headers%20with%20the,comma%2Dseparated%20list%20of%20values.
                     :headers (->> req .headers .iteratorAsString iterator-seq
                                (into {} (map (fn [[k v]] [(keyword k) v]))))
                     :cookies (some->>
                                (some-> req .headers (.get HttpHeaderNames/COOKIE))
                                (.decode ServerCookieDecoder/STRICT)
                                (into {} (map (fn [c] [(.name c) (.value c)]))))
                     ; TODO check content type and charset if specified... or just stream bytes?
                     ; Not really fair to expect application to do.
                     :content (some-> req .content (.toString CharsetUtil/UTF_8))}
                    (= method HttpMethod/POST)
                    (assoc :data
                           (into [] (for [^InterfaceHttpData bhd
                                          (-> req HttpPostRequestDecoder. .getBodyHttpDatas)]
                                      (condp instance? bhd
                                        ; Only if application/x-www-form-urlencoded ?
                                        Attribute [(.getName bhd) (.getValue ^Attribute bhd)]
                                        ; TODO https://gist.github.com/breznik/6215834 maybe streaming?
                                        ; Would need to disable HttpObjectAggregator?
                                        FileUpload [(.getName bhd)
                                                    (str "Unsupported file upload "
                                                      (.getFilename ^FileUpload bhd)
                                                      (.getContentType ^FileUpload bhd)
                                                      (.getContentTransferEncoding ^FileUpload bhd))]
                                        [(.getName bhd) (str "Unsupported http body data type "
                                                          (.getHttpDataType bhd))])))))
                  (fn [val]
                    (if val
                      (.read ctx) ; because autoRead is false
                      (log/error "Dropped incoming http request because in chan is closed"))))
              (let [; NB/FIXME this is blocking
                    {:keys [status headers cookies content]}
                    (alt!! (async/timeout 5000) {:status (.code HttpResponseStatus/SERVICE_UNAVAILABLE)}
                           out-sub ([v] v))
                    #_#__ (log/info "Got from alt!!")
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
                ; TODO trailing headers? for chunked responses? Interaction with HttpObjectAggregator?
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