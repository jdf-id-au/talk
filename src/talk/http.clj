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
                             ChannelHandlerContext ChannelFutureListener
                             DefaultFileRegion)
           (io.netty.handler.codec.http HttpUtil
                                        DefaultFullHttpResponse DefaultHttpResponse
                                        HttpResponseStatus
                                        FullHttpResponse
                                        HttpHeaderNames QueryStringDecoder HttpResponse
                                        HttpRequest HttpContent LastHttpContent HttpObject HttpVersion HttpHeaderValues HttpExpectationFailedEvent HttpMessage)
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
           (java.net URLConnection)
           (java.nio.charset Charset)))

; Bit redundant to spec incoming because Netty will have done some sanity checking.
; Still, good for clarity/gen/testing.
; Needs to harmonise with deftypes too.

; TODO make all more specific
(s/def ::protocol #{"" "HTTP/0.9" "HTTP/1.0" "HTTP/1.1"}) ; TODO HTTP/2.0 and followthrough!
(s/def ::keep-alive? boolean?)
(s/def ::meta (s/keys :opt-un [::keep-alive?]))
(s/def ::method #{:get :post :put :patch :delete :head :options :trace})
(s/def ::headers (s/map-of keyword? ; TODO lower kw
                   (s/or ::single string?
                     ::multiple (s/coll-of string? :kind vector?))))
(s/def ::cookies (s/map-of string? string?))
(s/def ::uri string?)
(s/def ::path string?)
(s/def ::query string?)
(s/def ::parameters map?)
; Permissive receive, doesn't enforce HTTP semantics
(s/def ::Request ; capitalised because defrecord
  (s/keys :req-un [:talk.server/ch ::protocol ::meta ::method ::headers ::cookies
                   ::uri ::path ::query ::parameters]))

(s/def ::name string?)
(s/def ::charset (s/with-gen (s/nilable #(instance? Charset %))
                   #(gen/fmap (fn [^String s] (Charset/forName s))
                      #{"ISO-8859-1" "UTF-8"})))
(s/def ::file? boolean?)
(s/def ::file (s/with-gen #(instance? java.io.File %)
                #(gen/fmap (fn [^String p] (java.io.File. p))
                   (gen/string))))
(s/def ::value (s/or ::data bytes? ::file ::file))
(s/def ::content-type string?)
(s/def ::transfer-encoding string?)

(s/def ::Attribute (s/keys :req-un [:talk.server/ch ::name ::charset ::file? ::value]))
(s/def ::File
  (s/keys :req-un [:talk.server/ch ::name ::charset ::content-type ::transfer-encoding ::file? ::value]))

(s/def ::cleanup fn?)
(s/def ::Trail
  (s/keys :req-un [:talk.server/ch ::cleanup ::headers]))

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
(s/def ::content (s/or ::file ::file ::string string? ::bytes bytes? ::nil nil?))
; TODO could enforce HTTP semantics in spec (e.g. (s/and ... checking-fn)
; (Some like CORS preflight might need to be stateful and therefore elsewhere... use Netty's impl!)
(s/def ::response (s/keys :req-un [::status] :opt-un [::headers ::cookies ::content]))

(def on #(.asShortText (:channel %)))

(defrecord Request [channel protocol meta method headers cookies uri path parameters]
  Object (toString [r] (str \< (-> method name str/upper-case) \  uri " on channel " (on r) \>)))
; file? explains whether value will be bytes or File
; keep charset as java.nio.Charset because convenient for decoding
(defrecord Attribute [channel name charset file? value]
  Object (toString [r] (str "<Attribute " name \  (when file? "written to disk") " from " (on r) \>)))
; TODO support PUT and PATCH ->File
(defrecord File [channel name charset content-type transfer-encoding file? value]
  Object (toString [r] (str "<FileUpload " name \  (when file? "written to disk") " from " (on r) \>)))
; trailing headers ; NB if echoing file, should delay cleanup until fully sent
(defrecord Trail [channel cleanup headers]
  Object (toString [r] (str "<Cleanup " headers \>)))


(defn code!
  "Send HTTP response with given status and empty content using HTTP/1.1 or given version."
  ([ctx status] (code! ctx HttpVersion/HTTP_1_1 status))
  ([^ChannelHandlerContext ctx ^HttpVersion version ^HttpResponseStatus status]
   (-> (.writeAndFlush ctx (DefaultFullHttpResponse. version status Unpooled/EMPTY_BUFFER))
       (.addListener ChannelFutureListener/CLOSE))))

; after https://netty.io/4.1/xref/io/netty/example/http/websocketx/server/WebSocketIndexPageHandler.html
(defn respond!
  "Send HTTP response, and manage keep-alive and Content-Length.
   Caller needs to ensure that next incoming message is read!"
  [^ChannelHandlerContext ctx keep-alive? ^FullHttpResponse res]
  (log/debug "Responding with" res keep-alive?)
  (let [status (.status res)
        ok-or-continue? (contains? #{HttpResponseStatus/OK HttpResponseStatus/CONTINUE} status)
        keep-alive? (and keep-alive? ok-or-continue?)]
    (HttpUtil/setKeepAlive res keep-alive?)
    ; May need to review when enabling HttpContentEncoder etc. What about HTTP/2?
    (HttpUtil/setContentLength res (-> res .content .readableBytes))
    (let [cf (.writeAndFlush ctx res)]
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))

(defn stream!
  [^ChannelHandlerContext ctx keep-alive? ^HttpResponse res ^java.io.File file]
   ; after https://github.com/datskos/ring-netty-adapter/blob/master/src/ring/adapter/plumbing.clj and current docs
  (assert (.isFile file))
  (log/debug "STREAMING" res)
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

(defn responder
  "Submit request to application and wait for its response, or timeout.
   Read next incoming message (provides backpressure)."
  [{:keys [clients handler-timeout state] :as opts}]
  (let [{:keys [^ChannelHandlerContext ctx ^HttpVersion protocol keep-alive?]} @state
        id (-> ctx .channel .id)
        out-sub (get-in @clients [id :out-sub])]
    (go
      (try
        (if-let [{:keys [status headers cookies content] :as res}
                 (alt! out-sub ([v] v) (async/timeout handler-timeout) nil)]
          (do
            (log/debug "TRYING TO SEND" res)
            (if (instance? java.io.File content)
              ; TODO add support for other streaming sources (use protocols?)
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
                    res (DefaultFullHttpResponse. ; needed protocol hinted to resolve?
                          protocol
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
              (code! ctx protocol HttpResponseStatus/SERVICE_UNAVAILABLE)))
        (catch Exception e
          (log/error "Error in http response handler" e)
          (code! ctx protocol HttpResponseStatus/INTERNAL_SERVER_ERROR))))))


(defn parse-headers
  "Return map containing maps of headers and cookies."
  [headers]
  (some->> headers .iteratorAsString iterator-seq
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
      {})))

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
                (when-not (async/put! in (->File (.id ch)
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

(defn dfhr [^HttpResponseStatus s & {:as headers}]
  (let [r (DefaultFullHttpResponse. HttpVersion/HTTP_1_1 s Unpooled/EMPTY_BUFFER)]
    (doseq [[k v] headers] (-> r .headers (.set ^CharSequence k ^Object v)))
    r))

(def response
  {:EXPECTATION_FAILED
    (dfhr HttpResponseStatus/EXPECTATION_FAILED
     HttpHeaderNames/CONTENT_LENGTH 0)
   :CONTINUE
    (dfhr HttpResponseStatus/CONTINUE)
   :TOO_LARGE
    (dfhr HttpResponseStatus/REQUEST_ENTITY_TOO_LARGE
     HttpHeaderNames/CONTENT_LENGTH 0)
   :TOO_LARGE_CLOSE
    (dfhr HttpResponseStatus/REQUEST_ENTITY_TOO_LARGE
     HttpHeaderNames/CONTENT_LENGTH 0
     HttpHeaderNames/CONNECTION HttpHeaderValues/CLOSE)})

(defn reuse [status]
  (-> response status .retainedDuplicate))

(defn manage-expectations
  "Like HttpObjectAggregator, but only manages Expect: 100-continue."
  [{:keys [out state max-content-length] :as opts} req]
  (let [ctx ^ChannelHandlerContext (:ctx @state)
        p (.pipeline ctx)
        keep-alive? (HttpUtil/isKeepAlive req)]
    (cond
      (-> req .protocolVersion (.compareTo HttpVersion/HTTP_1_1) (< 0))
      (do (log/debug "Wrong HTTP version")
          (.fireUserEventTriggered p HttpExpectationFailedEvent/INSTANCE)
          (respond! ctx keep-alive? (reuse :EXPECTATION_FAILED)))

      (> (HttpUtil/getContentLength req (cast Long 0)) max-content-length)
      (do (log/debug "Too large")
          (.fireUserEventTriggered p HttpExpectationFailedEvent/INSTANCE)
          (respond! ctx keep-alive? (reuse :TOO_LARGE)))

      :else
      (do (log/debug "Please continue")
          (async/put! out {:ch (-> ctx .channel .id) :status 100})
          (responder opts)))
          ; This would probably save a tiny amount of resources, but doesn't read from out-sub...
          ;(respond! ctx keep-alive? (reuse :CONTINUE))))
    (.read ctx)))

; Interface graph:
;   HttpObject
;     -> HttpContent
;          -> LastHttpContent
;               |-> FullHttpMessage
;     -> HttpMessage  |-> FullHttpRequest
;          -> HttpRequest

(defn ^ChannelHandler handler
  "Parse HTTP requests and forward to `in` with backpressure. Respond asynchronously from `out-sub`."
  ; TODO read about HTTP/2 https://developers.google.com/web/fundamentals/performance/http2
  [{:keys [state in disk-threshold max-content-length] :as opts}]
  (log/debug "Starting http handler with" opts)
  (let [data-factory (DefaultHttpDataFactory. ^long disk-threshold)]
    (set! (. DiskFileUpload deleteOnExitTemporaryFile) true) ; same as default
    (set! (. DiskFileUpload baseDirectory) nil) ; system temp directory
    (set! (. DiskAttribute deleteOnExitTemporaryFile) true)
    (set! (. DiskAttribute baseDirectory) nil)
    (proxy [SimpleChannelInboundHandler] [HttpObject]
      (channelRead0 [^ChannelHandlerContext ctx ^HttpObject obj]
        (log/debug "Received a" (.toString obj))
        ; FIXME defence against unwelcome requests (esp large PUT/POST)
        (when-let [^HttpRequest req (and (instance? HttpRequest obj) obj)]
          (when-not (some-> req .headers (.get HttpHeaderNames/CONTENT_TYPE))
            (-> req .headers (.set HttpHeaderNames/CONTENT_TYPE
                               HttpHeaderValues/APPLICATION_OCTET_STREAM)))
          (let [decoder (try (HttpPostRequestDecoder. data-factory req)
                             (catch HttpPostRequestDecoder$ErrorDataDecoderException e
                               (log/warn "Error setting up POST decoder" e)
                               (code! ctx HttpResponseStatus/UNPROCESSABLE_ENTITY)))
                protocol (.protocolVersion req)]
            (swap! state assoc :decoder decoder :protocol protocol
              ; Move this to ->Request meta if want application to be able to manipulate keep-alive
              :keep-alive? (HttpUtil/isKeepAlive req))
            (cond
              (-> req .decoderResult .isSuccess not)
              (do (log/warn (-> req .decoderResult .cause .getMessage))
                  (code! ctx HttpResponseStatus/BAD_REQUEST))

              (some-> req .headers (.get HttpHeaderNames/EXPECT)
                (.equalsIgnoreCase (.toString HttpHeaderValues/CONTINUE)))
              (manage-expectations opts req)

              (some-> req .headers (.get HttpHeaderNames/UPGRADE)
                (.equalsIgnoreCase (.toString HttpHeaderValues/WEBSOCKET)))
              (let [p (.pipeline ctx)]
                  ; This is probably unnecessary (and noop) because HttpRequest has no content
                  ; and so has no ByteBuf to pass back to pipeline!
                  (ReferenceCountUtil/retain req)
                  (log/debug "Passing ws upgrade request through http/handler.")
                  ; Remove self from pipeline for this channel!
                  (.remove p "http-handler")
                  (.fireChannelRead ctx req))

               ; (cast Long 0) worked but (long 0) didn't! maybe...
               ; https://stackoverflow.com/questions/12586881/clojure-overloaded-method-resolution-for-longs
              (> (HttpUtil/getContentLength req (cast Long 0)) max-content-length)
              (do (log/warn "Max content length exceeded")
                  (code! ctx HttpResponseStatus/REQUEST_ENTITY_TOO_LARGE))

              :else
              (let [qsd (-> req .uri QueryStringDecoder.)
                    {:keys [headers cookies]} (-> req .headers parse-headers)
                    protocol (.protocolVersion req)
                      ; Shouldn't really have body for GET, DELETE, TRACE, OPTIONS, HEAD
                      #_ (some-> req .content)]
                (if (async/put! in (->Request (-> ctx .channel .id)
                                     (.toString protocol)
                                     {:content-length (let [l (HttpUtil/getContentLength req (cast Long -1))]
                                                        (when-not (neg? l) l))
                                      :charset (HttpUtil/getCharset req)
                                      :content-type (HttpUtil/getMimeType req)}
                                     (-> req .method .toString str/lower-case keyword)
                                     headers cookies (.uri req) (.path qsd) (.parameters qsd)))
                  (responder opts)
                  (do (log/error "Dropped incoming http request because in chan is closed")
                      (code! ctx protocol HttpResponseStatus/SERVICE_UNAVAILABLE)))))))

        ; After https://github.com/netty/netty/blob/master/example/src/main/java/io/netty/example/http/upload/HttpUploadServerHandler.java
        ; TODO wait and see if HPRD can actually handle PUT and POST
        (when-let [^HttpPostRequestDecoder decoder (:decoder @state)]
          (log/debug "decoding")
          (when-let [^HttpContent con (and (instance? HttpContent obj) obj)]
            (try (.offer decoder con)
                 (catch HttpPostRequestDecoder$ErrorDataDecoderException e
                   (log/warn "Error running POST decoder" e)
                   (code! ctx HttpResponseStatus/UNPROCESSABLE_ENTITY)))
            (read-chunkwise opts)
            (when-let [^LastHttpContent con (and (instance? LastHttpContent obj) obj)]
              (if (async/put! in (->Trail (-> ctx .channel .id)
                                          #(.destroy decoder)
                                          (-> con .trailingHeaders parse-headers :headers)))
                (responder opts)
                (do (.destroy decoder)
                    (log/error "Couldn't deliver cleanup fn because in chan is closed. Cleaned up myself.")))))
          (.read ctx))
        (log/debug "End of http/handler. Did you read the next incoming message?"))
      (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
        (log/error "Error in http handler" cause)
        (some-> ^HttpPostRequestDecoder (:decoder @state) .destroy)
        (.close ctx)))))