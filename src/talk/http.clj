(ns talk.http
  (:require [talk.common :as common]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]])
  (:import (io.netty.buffer Unpooled ByteBuf)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler
                             ChannelHandlerContext ChannelFutureListener ChannelOption DefaultFileRegion)
           (io.netty.handler.codec.http HttpUtil
                                        DefaultFullHttpResponse DefaultHttpResponse
                                        HttpResponseStatus
                                        FullHttpRequest FullHttpResponse
                                        HttpHeaderNames QueryStringDecoder HttpMethod HttpResponse)
           (io.netty.util CharsetUtil)
           (io.netty.handler.codec.http.cookie ServerCookieDecoder ServerCookieEncoder)
           (io.netty.handler.codec.http.multipart HttpPostRequestDecoder
                                                  InterfaceHttpData
                                                  DefaultHttpDataFactory
                                                  FileUpload Attribute
                                                  DiskFileUpload DiskAttribute
                                                  MixedFileUpload MixedAttribute)
           (java.io File RandomAccessFile)
           (java.net URLConnection)))

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

(defn stream!
  [^ChannelHandlerContext ctx keep-alive? ^HttpResponse res ^File file]
   ; after https://github.com/datskos/ring-netty-adapter/blob/master/src/ring/adapter/plumbing.clj and current docs
  (assert (.isFile file))
  (let [len (.length file)
        region (DefaultFileRegion. file 0 len)
        ; NB breaks if no os support for zero-copy
        hdrs (.headers res)]
    (when-not (.get hdrs "Content-Type")
      (.set hdrs "Content-Type" (URLConnection/guessContentTypeFromName (.getName file))))
    (HttpUtil/setContentLength res len)
    ; TODO backpressure?
    ; FIXME write on channel or context?
    (.writeAndFlush ctx res) ; initial line and header
    (let [cf (.writeAndFlush ctx region)] ; encoded into several HttpContents?
      (.addListener cf
        (reify ChannelFutureListener ; FIXME progress? ChannelProgressiveFutureListener ?
          (operationComplete [_ _]
            (.release region))))
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))

(def post-handler
  "Handle HTTP POST data. Does not apply charset automatically.
   NB data >16kB will be stored in tempfiles, which will be lost on JVM shutdown.
   Must destroy decoder after response sent; this will also remove tempfiles."
  ; after https://gist.github.com/breznik/6215834
  ; TODO protect file uploads somehow
  (let [data-factory (DefaultHttpDataFactory.)] ; memory if <16kB, else disk
    (set! (. DiskFileUpload deleteOnExitTemporaryFile) true) ; same as default
    (set! (. DiskFileUpload baseDirectory) nil) ; system temp directory
    (set! (. DiskAttribute deleteOnExitTemporaryFile) true)
    (set! (. DiskAttribute baseDirectory) nil)
    (fn [req]
      (when (= (.method req) HttpMethod/POST)
        (let [decoder (HttpPostRequestDecoder. data-factory req)]
          {:post-decoder decoder
           :post-data
           (into {}
             (for [^InterfaceHttpData d (.getBodyHttpDatas decoder)]
               [(.getName d)
                (condp instance? d
                  ; Only if application/x-www-form-urlencoded ?
                  Attribute
                  (let [a ^MixedAttribute d
                        base {:charset (.getCharset a)}]
                    (if (.isInMemory a)
                      ; NB puts onus on application to apply charset
                      ; e.g. (slurp value :encoding "UTF-8")
                      (assoc base :value (.get a))
                      (assoc base :file (.getFile a))))
                  FileUpload
                  (let [f ^MixedFileUpload d
                        base {:charset (.getCharset f)
                              :client-filename (.getFilename f)
                              :content-type (.getContentType f)
                              :content-transfer-encoding (.getContentTransferEncoding f)
                              :charset (.getCharset f)}]
                    (if (.isInMemory f)
                      (assoc base :value (.get f))
                      (assoc base :file (.getFile f))))
                  (log/info "Unsupported http body data type " (.getHttpDataType d)))]))})))))

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
                  (do
                    (if (instance? File content) ; TODO add support for other streaming sources
                      ; Streaming:
                      (let [res (DefaultHttpResponse.
                                  protocol-version
                                  (HttpResponseStatus/valueOf status))
                            hdrs (.headers res)]
                        (doseq [[k v] headers] (.set hdrs (name k) v))
                        (.set hdrs HttpHeaderNames/SET_COOKIE ^Iterable ; TODO expiry?
                          (mapv #(.encode ServerCookieEncoder/STRICT (first %) (second %)) cookies))
                        ; TODO trailing headers?
                        (stream! ctx keep-alive? res content))
                      ; Non-streaming:
                      (let [buf (condp #(%1 %2) content
                                  string? (Unpooled/copiedBuffer ^String content CharsetUtil/UTF_8)
                                  nil? Unpooled/EMPTY_BUFFER
                                  bytes? (Unpooled/copiedBuffer ^bytes content))
                            res (DefaultFullHttpResponse.
                                  protocol-version
                                  (HttpResponseStatus/valueOf status)
                                  ^ByteBuf buf)
                            hdrs (.headers res)]
                        (doseq [[k v] headers] (.set hdrs (name k) v))
                        ; TODO need to support repeated headers (other than Set-Cookie ?)
                        (.set hdrs HttpHeaderNames/SET_COOKIE ^Iterable ; TODO expiry?
                          (mapv #(.encode ServerCookieEncoder/STRICT (first %) (second %)) cookies))
                        (respond! ctx keep-alive? res)))
                    (some-> post-decoder .destroy)
                    (.read ctx)) ; because autoRead is false
                  (do (log/error "Dropped incoming http request because of out chan timeout")
                      (some-> post-decoder .destroy)
                      (respond! ctx false (DefaultFullHttpResponse.
                                            protocol-version
                                            HttpResponseStatus/SERVICE_UNAVAILABLE
                                            Unpooled/EMPTY_BUFFER))))
                (catch Exception e
                  (log/error "Error in http response handler" e)
                  (some-> post-decoder .destroy)
                  (respond! ctx false (DefaultFullHttpResponse.
                                        protocol-version
                                        HttpResponseStatus/INTERNAL_SERVER_ERROR
                                        Unpooled/EMPTY_BUFFER))))
              (do (log/error "Dropped incoming http request because in chan is closed")
                  (some-> post-decoder .destroy)
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