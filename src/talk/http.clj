(ns talk.http
  (:require [talk.common :as common]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]]
            [clojure.string :as str])
  (:import (io.netty.buffer Unpooled ByteBuf)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler
                             ChannelHandlerContext ChannelFutureListener ChannelOption
                             DefaultFileRegion)
           (io.netty.handler.codec.http HttpUtil
                                        DefaultFullHttpResponse DefaultHttpResponse
                                        HttpResponseStatus
                                        FullHttpRequest FullHttpResponse
                                        HttpHeaderNames QueryStringDecoder HttpMethod HttpResponse)
           (io.netty.util CharsetUtil)
           (io.netty.handler.codec.http.cookie ServerCookieDecoder ServerCookieEncoder Cookie)
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
  (let [raf (RandomAccessFile. file "r")
        len (.length raf)
        ; connection seems to close prematurely when using DFR directly on file - because lazy open?
        region (DefaultFileRegion. (.getChannel raf) 0 len) #_(DefaultFileRegion. file 0 len)
        ; NB breaks if no os support for zero-copy
        hdrs (.headers res)]
    (when-not (.get hdrs HttpHeaderNames/CONTENT_TYPE)
      (some->> file .getName URLConnection/guessContentTypeFromName
        (.set hdrs HttpHeaderNames/CONTENT_TYPE)))
    (HttpUtil/setContentLength res len)
    ; TODO backpressure?
    (.writeAndFlush ctx res) ; initial line and header
    (let [cf (.writeAndFlush ctx region)] ; encoded into several HttpContents?
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))

(def upload-handler
  "Handle HTTP POST/PUT/PATCH data. Does not apply charset automatically.
   NB data >16kB will be stored in tempfiles, which will be lost on JVM shutdown.
   Must cleanup after response sent; this will also remove tempfiles."
  ; after https://gist.github.com/breznik/6215834
  ; TODO protect against file upload abuse somehow
  (let [data-factory (DefaultHttpDataFactory.)] ; memory if <16kB, else disk
    (set! (. DiskFileUpload deleteOnExitTemporaryFile) true) ; same as default
    (set! (. DiskFileUpload baseDirectory) nil) ; system temp directory
    (set! (. DiskAttribute deleteOnExitTemporaryFile) true)
    (set! (. DiskAttribute baseDirectory) nil)
    (fn [^FullHttpRequest req]
      (condp contains? (.method req)
        #{HttpMethod/POST}
        (let [decoder (HttpPostRequestDecoder. data-factory req)]
          {:cleanup #(.destroy decoder)
           :data
           (into []
             (for [^InterfaceHttpData d (.getBodyHttpDatas decoder)
                   :let [base {:name (.getName d)}]]
               (condp instance? d
                 ; Only if application/x-www-form-urlencoded ?
                 Attribute
                 (let [a ^MixedAttribute d
                       base (assoc base :charset (.getCharset a))]
                   (if (.isInMemory a)
                     ; NB puts onus on application to apply charset
                     ; e.g. (slurp value :encoding "UTF-8")
                     (assoc base :value (.get a))
                     (assoc base :file (.getFile a))))
                 FileUpload
                 (let [f ^MixedFileUpload d
                       base (assoc base :charset (.getCharset f)
                                        :client-filename (.getFilename f)
                                        :content-type (.getContentType f)
                                        :content-transfer-encoding (.getContentTransferEncoding f))]
                   (if (.isInMemory f)
                     (assoc base :value (.get f))
                     (assoc base :file (.getFile f))))
                 (log/info "Unsupported http body data type "
                   (assoc base :type (.getHttpDataType d))))))})
        #{HttpMethod/PUT HttpMethod/PATCH}
        (let [mime-type (or (HttpUtil/getMimeType req) "application/octet-stream")
              charset (HttpUtil/getCharset req)
              content-length (HttpUtil/getContentLength req)
              base {:charset charset}
              u (doto (.createFileUpload data-factory
                        req "PUT data" "see path" mime-type "binary" charset content-length)
                      ; https://stackoverflow.com/a/41572878/780743
                      (.setContent (.content (.retain req))))]
          {:cleanup #(.cleanRequestHttpData data-factory req)
           :data (vector (if (.isInMemory u)
                           (assoc base :value (.get u))
                           (assoc base :file (.getFile u))))})
        ; else
        nil))))

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
              basics {:ch id
                      :method (-> method .toString keyword)
                      :path (.path qsd)
                      :query (.parameters qsd)
                      :protocol (-> req .protocolVersion .toString)}
              {:keys [headers cookies]}
              (some->> req .headers .iteratorAsString iterator-seq
                (reduce
                  ; Are repeated headers already coalesced by netty?
                  ; Does it handle keys case-sensitively?
                  (fn [m [k v]]
                    (let [lck (-> k str/lower-case keyword)]
                      (case lck
                        :cookie
                        (update m :cookies into
                          (for [^Cookie c (.decode ServerCookieDecoder/STRICT v)]
                            ; TODO could look at max-age, etc...
                            [(.name c) (.value c)]))
                        (update m :headers
                          (fn [hs]
                            (if-let [old (get hs lck)]
                              (if (vector? old)
                                (conj old v)
                                [old v])
                              (assoc hs lck v)))))))
                  {}))
              {:keys [data cleanup] :or {cleanup (constantly nil)}} (upload-handler req)
              _ (log/debug "data" data)
              request-map
              (cond-> (assoc basics :headers headers :cookies cookies)
                data (assoc :data data)
                (not data) (assoc :content (some-> req .content
                                             (.toString (HttpUtil/getCharset req)))))]
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
                        (doseq [[k v] headers] (.set hdrs (-> k name str/lower-case) v))
                        ; TODO need to support repeated headers (other than Set-Cookie ?)
                        (.set hdrs HttpHeaderNames/SET_COOKIE ^Iterable ; TODO expiry?
                          (mapv #(.encode ServerCookieEncoder/STRICT (first %) (second %)) cookies))
                        (respond! ctx keep-alive? res)))
                    (cleanup)
                    (.read ctx)) ; because autoRead is false
                  (do (log/error "Dropped incoming http request because of out chan timeout")
                      (cleanup)
                      (respond! ctx false (DefaultFullHttpResponse.
                                            protocol-version
                                            HttpResponseStatus/SERVICE_UNAVAILABLE
                                            Unpooled/EMPTY_BUFFER))))
                (catch Exception e
                  (log/error "Error in http response handler" e)
                  (cleanup)
                  (respond! ctx false (DefaultFullHttpResponse.
                                        protocol-version
                                        HttpResponseStatus/INTERNAL_SERVER_ERROR
                                        Unpooled/EMPTY_BUFFER))))
              (do (log/error "Dropped incoming http request because in chan is closed")
                  (cleanup)
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