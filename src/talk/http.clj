(ns talk.http
  "Parse http requests and forward to `in` with backpressure.
   Respond asynchronously from `out-sub` or timeout."
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen])
  (:import (io.netty.buffer Unpooled ByteBuf)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler
                             ChannelHandlerContext ChannelFutureListener ChannelOption
                             DefaultFileRegion)
           (io.netty.handler.codec.http HttpUtil
                                        DefaultFullHttpResponse DefaultHttpResponse
                                        HttpResponseStatus
                                        FullHttpResponse
                                        HttpHeaderNames QueryStringDecoder HttpResponse
                                        HttpRequest HttpContent LastHttpContent HttpObject HttpVersion)
           (io.netty.util CharsetUtil ReferenceCountUtil)
           (io.netty.handler.codec.http.cookie ServerCookieDecoder ServerCookieEncoder Cookie)
           (io.netty.handler.codec.http.multipart HttpPostRequestDecoder
                                                  InterfaceHttpData
                                                  DefaultHttpDataFactory
                                                  DiskFileUpload DiskAttribute
                                                  HttpPostRequestDecoder$ErrorDataDecoderException
                                                  HttpPostRequestDecoder$EndOfDataDecoderException
                                                  InterfaceHttpData$HttpDataType FileUpload)
           (java.io RandomAccessFile)
           (java.net URLConnection InetSocketAddress)
           (io.netty.channel.group ChannelGroup)))

; Bit redundant to spec incoming because Netty will have done some sanity checking.
; Still, good for clarity/gen/testing.
(s/def ::protocol #{"" "HTTP/0.9" "HTTP/1.0" "HTTP/1.1"}) ; TODO HTTP/2.0 and followthrough!
(s/def ::method #{:get :post :put :patch :delete :head :options :trace}) ; ugh redundant
(s/def ::path string?) ; TODO improve
(s/def ::query string?) ;
(s/def ::headers (s/map-of keyword? ; TODO lower kw
                   (s/or ::single string?
                         ::multiple (s/coll-of string? :kind vector?))))
(s/def ::cookies (s/map-of string? string?))

(s/def ::name string?)
(s/def ::client-filename string?)
(s/def ::content-type string?)
(s/def ::content-transfer-encoding string?)
(s/def ::charset (s/nilable keyword?))
(s/def ::value any?)
(s/def ::file (s/with-gen #(instance? File %) #(gen/fmap (fn [^String p] (File. p))
                                                 (gen/string))))
(s/def ::type any?)
(s/def ::attachment (s/keys :req-un [(or (and ::name ::charset (or ::value ::file))
                                         (and ::name ::charset ::client-filename
                                              ::content-type ::content-transfer-encoding
                                              (or ::value ::file))
                                         (and ::name ::type)
                                         (and ::charset (or ::value ::file)))]))
(s/def ::data (s/nilable (s/coll-of ::attachment :kind vector?)))
(s/def ::content (s/or ::file ::file ::string string? ::bytes bytes? ::nil nil?))

; Permissive receive, doesn't enforce HTTP semantics
(s/def ::request (s/keys :req-un [:talk.api/ch ::protocol ::method ::path ::query ::headers]
                         :opt-un [::cookies ::data]))

(s/def ::status #{; See HttpResponseStatus
                  100 101 102 ; unlikely to use 100s at application level
                  200 201 202 203 204 205 206 207
                  300 301 302 303 304 305     307 308
                  400 401 402 403 404 405 406 407 408 409
                  410 411 412 413 414 415 416 417 ; 418 I'm a teapot
                      421 422 423 424 425 426     428 429
                      431
                  500 501 502 503 504 505 506 507
                  510 511})

; TODO could enforce HTTP semantics in spec (e.g. (s/and ... checking-fn)
; (Some like CORS preflight might need to be stateful and therefore elsewhere... use Netty's impl!)
(s/def ::response (s/keys :req-un [::status]
                          :opt-un [::headers ::cookies ::content]))

(defn track-channel
  "Register channel in `clients` map and report on `in` chan.
   Map entry is a map containing `type`, `out-sub` and `addr`, and can be updated.

   Usage:
   - Call from channelActive.
   - Detect websocket upgrade handshake, using userEventTriggered, and update `clients` map."
  [{:keys [^ChannelGroup channel-group
           state clients in out-pub]}]
  (let [ctx ^ChannelHandlerContext (:ctx @state)
        ch (.channel ctx)
        id (.id ch)
        cf (.closeFuture ch)
        out-sub (chan)]
    (try (.add channel-group ch)
         (async/sub out-pub id out-sub)
         (swap! clients assoc id
           :type :http ; changed in userEventTriggered
           :out-sub out-sub
           :addr (-> ch ^InetSocketAddress .remoteAddress .getAddress HttpUtil/formatHostnameForHttp))
         (when-not (put! in {:ch id :type :talk.api/connection :connected true})
           (log/error "Unable to report connection because in chan is closed"))
         (.addListener cf
           (reify ChannelFutureListener
             (operationComplete [_ _]
               (swap! clients dissoc id)
               (when-not (put! in {:ch id :type :talk.api/connection :connected false})
                 (log/error "Unable to report disconnection because in chan is closed")))))
         (catch Exception e
           (log/error "Unable to register channel" ch e)
           (throw e)))))

(defn code!
  "Send HTTP response with given status and empty content using HTTP/1.1 or given version."
  ([ctx status] (code! ctx HttpVersion/HTTP_1_1 status))
  ([^ChannelHandlerContext ctx ^HttpVersion version ^HttpResponseStatus status]
   (-> (.writeAndFlush ctx (DefaultFullHttpResponse. version status Unpooled/EMPTY_BUFFER))
       (.addListener ChannelFutureListener/CLOSE))))

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
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))

(defn stream!
  [^ChannelHandlerContext ctx keep-alive? ^HttpResponse res ^java.io.File file]
   ; after https://github.com/datskos/ring-netty-adapter/blob/master/src/ring/adapter/plumbing.clj and current docs
  (assert (.isFile file))
  (let [status (.status res)
        ok? (= status HttpResponseStatus/OK)
        keep-alive? (and keep-alive? ok?)
        raf (RandomAccessFile. file "r")
        len (.length raf)
        ; connection seems to close prematurely when using DFR directly on file - because lazy open?
        region (DefaultFileRegion. (.getChannel raf) 0 len)
               #_(DefaultFileRegion. file 0 len) #_ (ChunkedFile. raf)
        ; NB breaks if no os support for zero-copy; might not work with *netty's* ssl
        hdrs (.headers res)]
    (HttpUtil/setKeepAlive res keep-alive?)
    (HttpUtil/setContentLength res len)
    (when-not (.get hdrs HttpHeaderNames/CONTENT_TYPE)
      (some->> file .getName URLConnection/guessContentTypeFromName
        (.set hdrs HttpHeaderNames/CONTENT_TYPE)))
    ; TODO backpressure?
    ; TODO trailing headers?
    (.writeAndFlush ctx res) ; initial line and header
    (let [cf (.writeAndFlush ctx region)] ; encoded into several HttpContents?
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))

(defn responder [{:keys [in clients handler-timeout state] :as opts}
                 {:keys [protocol meta] :as req}]
  (let [^ChannelHandlerContext ctx (:ctx @state)
        {:keys [keep-alive?]} meta
        id (-> ctx .channel .id)
        out-sub (get-in @clients [id :out-sub])]
    (go
      (if (>! in req)
        (try
          (if-let [{:keys [status headers cookies content]}
                   (alt! out-sub ([v] v) (async/timeout handler-timeout) nil)]
            (do
              (if (instance? File content) ; TODO add support for other streaming sources
                ; Streaming:
                (let [res (DefaultHttpResponse. protocol
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
                      res (DefaultFullHttpResponse. protocol
                            (HttpResponseStatus/valueOf status)
                            ^ByteBuf buf)
                      hdrs (.headers res)]
                  (doseq [[k v] headers] (.set hdrs (-> k name str/lower-case) v))
                  ; TODO need to support repeated headers (other than Set-Cookie ?)
                  (.set hdrs HttpHeaderNames/SET_COOKIE ^Iterable ; TODO expiry?
                    (mapv #(.encode ServerCookieEncoder/STRICT (first %) (second %)) cookies))
                  (respond! ctx keep-alive? res)))
              (.read ctx)) ; because autoRead is false
            (do (log/error "Dropped incoming http request because of out chan timeout")
                (code! ctx HttpResponseStatus/SERVICE_UNAVAILABLE)))
          (catch Exception e
            (log/error "Error in http response handler" e)
            (code! ctx HttpResponseStatus/INTERNAL_SERVER_ERROR)))
        (do (log/error "Dropped incoming http request because in chan is closed")
            (code! ctx HttpResponseStatus/SERVICE_UNAVAILABLE))))))

(defrecord Request [channel protocol meta method headers cookies uri path parameters])
; file? explains whether value will be bytes or File
; keep charset as java.nio.Charset because convenient for decoding
(defrecord Attribute [channel name charset file? value])
; TODO support PUT and PATCH ->File
(defrecord File [channel name charset content-type transfer-encoding file? value])
; trailing headers
(defrecord Trail [channel cleanup headers])

(defn read-chunkwise [{:keys [state in] :as opts}]
  ; TODO work out how to indicate logged errors is while loop to user. Throw and catch? Cleanup?
  (let [^HttpPostRequestDecoder decoder (:decoder @state)
        ^ChannelHandlerContext ctx (:ctx @state)
        ch (.channel ctx)]
    (try
      (while (.hasNext decoder)
        (when-let [^InterfaceHttpData data (.next decoder)]
          (when (= data (:partial @state)) (swap! state dissoc :partial))
          (condp = (.getHttpDataType data)
            InterfaceHttpData$HttpDataType/Attribute
            (let [^io.netty.handler.codec.http.multipart.Attribute d data ; longhand to avoid clash
                  file? (-> d .isInMemory not)]
              (when-not (async/put! in (->Attribute (.id ch)
                                         (.getName d) (.getCharset d)
                                         file? (if file? (.getFile d) (.get d))))
                (log/error "Dropped incoming POST attribute because in chan is closed")))
            InterfaceHttpData$HttpDataType/FileUpload
            (let [^FileUpload d data file? (-> d .isInMemory not)]
              (if (.isCompleted d)
                (if (async/put! in (->File (.id ch)
                                     (.getFilename d) (.getCharset d)
                                     (.getContentType d) (.getContentTransferEncoding d)
                                     file? (if file? (.getFile d) (.get d))))
                  (log/error "Dropped incoming POST file because in chan is closed"))
                (log/info "Dropped incoming POST file because upload incomplete")))
            (log/info "Dropped incoming POST data because unrecognised type"))))
      (when-let [^InterfaceHttpData data (.currentPartialHttpData decoder)]
        (when-not (:partial @state)
          (swap! state assoc :partial data)))
          ; TODO could do finer-grained logging/events etc
      (catch HttpPostRequestDecoder$EndOfDataDecoderException e
        (log/info (.getMessage e))))))

(defn ^ChannelHandler handler
  "Parse HTTP requests and forward to `in` with backpressure. Respond asynchronously from `out-sub`."
  ; TODO read about HTTP/2 https://developers.google.com/web/fundamentals/performance/http2
  [{:keys [state in disk-threshold max-content-length] :as opts}]
  (let [data-factory (DefaultHttpDataFactory. ^long disk-threshold)]
    (set! (. DiskFileUpload deleteOnExitTemporaryFile) true) ; same as default
    (set! (. DiskFileUpload baseDirectory) nil) ; system temp directory
    (set! (. DiskAttribute deleteOnExitTemporaryFile) true)
    (set! (. DiskAttribute baseDirectory) nil)
    (proxy [SimpleChannelInboundHandler] [HttpObject]
      (channelActive [^ChannelHandlerContext ctx]
        ; TODO is this safe?
        ; Doesn't a given channel keep the same context instance, which is set by the pipeline in initChannel?
        (swap! state assoc :ctx ctx)
        (track-channel opts)
        (-> ctx .channel .config
              ; facilitate backpressure on subsequent reads; requires .read see branches below
          (-> (.setAutoRead false)
              ; May be needed for response from outside netty event loop:
              ; https://stackoverflow.com/a/48128514/780743
              (.setOption ChannelOption/ALLOW_HALF_CLOSURE true)))
        (.read ctx)) ; first read
      (channelRead0 [^ChannelHandlerContext ctx ^HttpObject obj]
        (when-let [^HttpRequest req (and (instance? HttpRequest obj) obj)]
          (if-not (-> req .decoderResult .isSuccess)
            (do (log/warn (-> req .decoderResult .cause .getMessage))
                (code! ctx HttpResponseStatus/BAD_REQUEST))
            (if (> (HttpUtil/getContentLength req 0) max-content-length)
              (do (log/warn "Max content length exceeded")
                  (code! ctx HttpResponseStatus/REQUEST_ENTITY_TOO_LARGE))
              (let [qsd (-> req .uri QueryStringDecoder.)
                    {:keys [headers cookies]}
                    (some->> req .headers .iteratorAsString iterator-seq
                      (reduce
                        ; Are repeated headers already coalesced by netty?
                        ; Does it handle keys case-sensitively?
                        (fn [m [k v]]
                          (let [lck (-> k str/lower-case keyword)]
                            (case lck
                              :cookie ; TODO omit if empty
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
                    decoder (try (HttpPostRequestDecoder. data-factory req)
                                 (catch HttpPostRequestDecoder$ErrorDataDecoderException e
                                   (log/warn (.getMessage e))
                                   (code! ctx HttpResponseStatus/UNPROCESSABLE_ENTITY)))
                      ; Shouldn't really have body for GET, DELETE, TRACE, OPTIONS, HEAD
                      #_ (some-> req .content)]
                (swap! @state assoc :decoder decoder)
                (responder opts
                  (->Request (-> ctx .channel .id)
                             (-> req .protocolVersion .toString)
                             {:keep-alive? (HttpUtil/isKeepAlive req)
                              :content-length (let [l (HttpUtil/getContentLength req -1)]
                                                (when-not (neg? l) l))
                              :charset (HttpUtil/getCharset req)
                              :content-type (HttpUtil/getMimeType req)}
                             (-> req .method .toString str/lower-case keyword)
                             headers cookies (.uri req) (.path qsd) (.parameters qsd)))))))
        ; After https://github.com/netty/netty/blob/master/example/src/main/java/io/netty/example/http/upload/HttpUploadServerHandler.java
        ; TODO wait and see if HPRD can actually handle PUT and POST
        (when-let [^HttpPostRequestDecoder decoder (:decoder @state)]
          (when-let [^HttpContent con (and (instance? HttpContent obj) obj)]
            (try (.offer decoder con)
                 (catch HttpPostRequestDecoder$ErrorDataDecoderException e
                   (log/warn (.getMessage e))
                   (code! ctx HttpResponseStatus/UNPROCESSABLE_ENTITY)))
            (read-chunkwise opts)
            (when-let [^LastHttpContent con (and (instance? LastHttpContent obj) obj)]
              (when-not (async/put! in (->Trail (-> ctx .channel .id)
                                                #(.destroy decoder)
                                                (.trailingHeaders con)))
                (.destroy decoder)
                (log/error "Couldn't deliver cleanup fn because in chan is closed. Cleaned up myself."))))))
      (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
        (log/error "Error in http handler" cause)
        (some-> ^HttpPostRequestDecoder (:decoder @state) .destroy)
        (.close ctx)))))