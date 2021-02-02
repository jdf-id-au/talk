(ns talk.http
  (:require [talk.common :as common]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]])
  (:import (io.netty.buffer Unpooled ByteBuf)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler
                             ChannelHandlerContext ChannelFutureListener ChannelOption)
           (io.netty.handler.codec.http HttpUtil
                                        DefaultFullHttpResponse
                                        HttpResponseStatus
                                        FullHttpRequest FullHttpResponse
                                        HttpHeaderNames QueryStringDecoder HttpMethod)
           (io.netty.util CharsetUtil)
           (io.netty.handler.codec.http.cookie ServerCookieDecoder ServerCookieEncoder)
           (io.netty.handler.codec.http.multipart HttpPostRequestDecoder Attribute
                                                  InterfaceHttpData FileUpload DefaultHttpDataFactory DiskFileUpload DiskAttribute HttpDataFactory MemoryAttribute MixedAttribute MixedFileUpload InterfaceHttpPostRequestDecoder)))

; after https://netty.io/4.1/xref/io/netty/example/http/websocketx/server/WebSocketIndexPageHandler.html
(defn respond!
  "Send HTTP response, and manage keep-alive and Content-Length."
  [^ChannelHandlerContext ctx keep-alive? ^FullHttpResponse res]
  (let [status (.status res)
        ok? (= status HttpResponseStatus/OK)
        keep-alive? (and keep-alive? ok?)]
    (HttpUtil/setKeepAlive res keep-alive?)
    ; May need to review when enabling HttpContentEncoder etc. What about HTTP/2?
    (HttpUtil/setContentLength res (-> res .content .readableBytes))
    (let [cf (.writeAndFlush ctx res)]
      ; Strictly should wait for this before alt! takes next from out-sub?
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))

(def post-handler
  "Handle HTTP POST data.
   NB data >16kB will be stored in tempfiles, which will be lost on JVM shutdown.
   Must destroy decoder after response sent; this will also remove tempfiles."
  (let [; after https://gist.github.com/breznik/6215834
        ; TODO protect file uploads somehow
        data-factory (DefaultHttpDataFactory.) ; memory if <16kB, else disk
        _ (set! (. DiskFileUpload deleteOnExitTemporaryFile) true) ; same as default
        _ (set! (. DiskFileUpload baseDirectory) nil) ; system temp directory
        _ (set! (. DiskAttribute deleteOnExitTemporaryFile) true)
        _ (set! (. DiskAttribute baseDirectory) nil)]
    (fn [req]
      (when (= (.method req) HttpMethod/POST)
        (let [decoder (HttpPostRequestDecoder. data-factory req)
              parsed
              (into {}
                (for [^InterfaceHttpData d (.getBodyHttpDatas decoder)
                      :let [base {:type (.getHttpDataType d)}]]
                  [(.getName d)
                   (condp instance? d
                     ; Only if application/x-www-form-urlencoded ?
                     Attribute
                     (let [a ^MixedAttribute d]
                       (if (.isInMemory a)
                         (assoc base :value (.get a))
                         (assoc base :file (.getFile a))))
                     FileUpload
                     (let [f ^MixedFileUpload d
                           fb (assoc base
                                :client-filename (.getFilename f)
                                :content-type (.getContentType f)
                                :content-transfer-encoding (.getContentTransferEncoding f))]
                       (if (.isInMemory f)
                         (assoc fb :value (.get f))
                         (assoc fb :file (.getFile f))))
                     (do (log/info "Unsupported http body data type " (.getHttpDataType d))
                         base))]))]
          {:post-data parsed :post-decoder decoder})))))

(defn ^ChannelHandler handler
  "Parse HTTP requests and forward to `in` with backpressure. Respond asynchronously from `out-sub`."
  ; TODO read about HTTP/2 https://developers.google.com/web/fundamentals/performance/http2
  [{:keys [clients in handler-timeout]
    :or {handler-timeout (* 5 1000)}
    :as admin}]
  (proxy [SimpleChannelInboundHandler] [FullHttpRequest]
    (channelActive [^ChannelHandlerContext ctx]
      ; facilitate backpressure on subsequent reads; requires .read see branches below
      (-> ctx .channel .config (-> (.setAutoRead false)
                                   ; May be needed for response from outside netty event loop:
                                   ; https://stackoverflow.com/a/48128514/780743
                                   (.setOption ChannelOption/ALLOW_HALF_CLOSURE true)))
      (common/track-channel ctx admin)
      (.read ctx)) ; first read
    (channelRead0 [^ChannelHandlerContext ctx ^FullHttpRequest req]
      (if (-> req .decoderResult .isSuccess)
        (let [ch (.channel ctx)
              id (.id ch)
              out-sub (get-in @clients [id :out-sub])
              method (.method req)
              qsd (-> req .uri QueryStringDecoder.)
              keep-alive? (HttpUtil/isKeepAlive req)
              protocol-version (.protocolVersion req) ; get info from req before released (necessary?)
              {:keys [post-data ^HttpPostRequestDecoder post-decoder]} (post-handler req)
              request-map
              (cond-> {:ch id
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
                post-data (assoc :data post-data))]
          (go
            (if (>! in request-map)
              (try
                (if-let [{:keys [status headers cookies content]}
                         (alt! out-sub ([v] v) (async/timeout handler-timeout) nil)]
                  ; TODO zero copy file streaming response after
                  ; https://github.com/datskos/ring-netty-adapter/blob/master/src/ring/adapter/plumbing.clj
                  (let [buf (condp #(%1 %2) content
                              string? (Unpooled/copiedBuffer ^String content CharsetUtil/UTF_8)
                              nil? Unpooled/EMPTY_BUFFER
                              bytes? (Unpooled/copiedBuffer ^bytes content))
                        res (DefaultFullHttpResponse.
                              protocol-version
                              (HttpResponseStatus/valueOf status)
                              ^ByteBuf buf)
                        hdrs (.headers res)]
                    (doseq [[k v] headers]
                      (.set hdrs (name k) v))
                    ; TODO need to support repeated headers (other than Set-Cookie ?)
                    ; TODO trailing headers? for chunked responses? Interaction with HttpObjectAggregator?
                    (.set hdrs HttpHeaderNames/SET_COOKIE ^Iterable ; TODO expiry?
                      (mapv #(.encode ServerCookieEncoder/STRICT (first %) (second %)) cookies))
                    (respond! ctx keep-alive? res)
                    (when post-decoder (.destroy post-decoder))
                    (.read ctx)) ; because autoRead is false
                  (do (log/error "Dropped incoming http request because of out chan timeout")
                      (when post-decoder (.destroy post-decoder))
                      (respond! ctx false (DefaultFullHttpResponse.
                                            protocol-version
                                            HttpResponseStatus/SERVICE_UNAVAILABLE
                                            Unpooled/EMPTY_BUFFER))))
                (catch Exception e
                  (log/error "Error in http response handler" e)
                  (when post-decoder (.destroy post-decoder))
                  (respond! ctx false (DefaultFullHttpResponse.
                                        protocol-version
                                        HttpResponseStatus/INTERNAL_SERVER_ERROR
                                        Unpooled/EMPTY_BUFFER))))
              (do (log/error "Dropped incoming http request because in chan is closed")
                  (when post-decoder (.destroy post-decoder))
                  (respond! ctx false (DefaultFullHttpResponse.
                                        protocol-version
                                        HttpResponseStatus/SERVICE_UNAVAILABLE
                                        Unpooled/EMPTY_BUFFER))))))
        (do (log/info "Decoder failure" (-> req .decoderResult .cause))
            (respond! ctx false (DefaultFullHttpResponse.
                                  (.protocolVersion req)
                                  HttpResponseStatus/BAD_REQUEST
                                  Unpooled/EMPTY_BUFFER)))))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
      (log/error "Error in http handler" cause)
      (.close ctx))))