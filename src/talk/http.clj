(ns talk.http
  "Parse http requests and forward to `in` with backpressure.
   Respond asynchronously from `out-sub` or timeout."
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [talk.util :refer [on briefly ess]])
  (:import (io.netty.buffer Unpooled ByteBuf)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler
                             ChannelHandlerContext ChannelFutureListener
                             DefaultFileRegion)
           (io.netty.handler.codec.http HttpUtil
                                        DefaultFullHttpResponse DefaultHttpResponse
                                        HttpResponseStatus
                                        FullHttpResponse
                                        HttpHeaderNames QueryStringDecoder HttpResponse
                                        HttpRequest HttpContent LastHttpContent HttpObject HttpVersion
                                        HttpHeaderValues FullHttpRequest)
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
           (java.net InetSocketAddress)
           (java.nio.charset Charset)
           (io.netty.channel.group ChannelGroup)))

; Spec and records

(s/def ::state #{:http :ws nil})
(s/def ::Connection (s/keys :req-un [:talk.server/channel ::state]))
(defrecord Connection [channel state]
  Object (toString [r] (str "Channel " (on r) \  (case state :http "open for http"
                                                             :ws "upgraded to ws"
                                                             nil "closed"))))

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

(s/def ::name string?)
(s/def ::charset (s/with-gen (s/nilable #(instance? Charset %))
                   #(gen/fmap (fn [^String s] (Charset/forName s))
                      (s/gen #{"ISO-8859-1" "UTF-8"}))))
(s/def ::file? boolean?)
(s/def ::file (s/with-gen #(instance? java.io.File %)
                #(gen/fmap (fn [^String p] (java.io.File. p))
                   (gen/string))))
(s/def ::value (s/or ::data bytes? ::file ::file))
(s/def ::content-type string?)
(s/def ::transfer-encoding string?)

(s/def ::cleanup (s/with-gen fn? #(gen/return any?)))


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
(s/def ::content (s/nilable (s/or ::file ::file ::string string? ::bytes bytes?)))
; TODO could enforce HTTP semantics in spec (e.g. (s/and ... checking-fn)
; (Some like CORS preflight might need to be stateful and therefore elsewhere... use Netty's impl!)
; `::response` is lowercase because no corresponding record because unnecessary.
(s/def ::response (s/keys :req-un [:talk.server/channel ::status] :opt-un [::headers ::cookies ::content]))

; Permissive receive, doesn't enforce HTTP semantics; capitalised because defrecord
(s/def ::Request (s/keys :req-un [:talk.server/channel ::protocol ::meta ::method ::headers ::cookies
                                  ::uri ::path ::query ::parameters]))
(defrecord Request [channel protocol meta method headers cookies uri path parameters]
  Object (toString [r] (str \< (-> method name str/upper-case) \  uri " on channel " (on r) \>)))
; file? explains whether value will be bytes or File
; keep charset as java.nio.Charset because convenient for decoding
(s/def ::Attribute (s/keys :req-un [:talk.server/channel ::name ::charset ::file? ::value]))
(defrecord Attribute [channel name charset file? value]
  Object (toString [r] (str "<Attribute " name \  (when file? "written to disk") " from " (on r) \>)))
; TODO support PUT and PATCH ->File
(s/def ::File (s/keys :req-un [:talk.server/channel ::name ::charset ::content-type ::transfer-encoding ::file? ::value]))
(defrecord File [channel name charset content-type transfer-encoding file? value]
  Object (toString [r] (str "<File " name \  (when file? "written to disk") " from " (on r) \>)))
; trailing headers ; NB if echoing file, should delay cleanup until fully sent
(s/def ::Trail (s/keys :req-un [:talk.server/channel ::cleanup ::headers]))
(defrecord Trail [channel cleanup headers]
  Object (toString [r] (str "<Trail " headers \>)))

; Maintain clients registry

(defn track-channel
  "Register channel in `clients` map and report on `in` chan.
   Map entry is a map containing `type`, `out-sub` and `addr`, and can be updated.

   Usage:
   - Call from channelActive.
   - Detect websocket upgrade handshake, using userEventTriggered, and update `clients` map."
  [{:keys [^ChannelGroup channel-group clients in out-pub]} ctx]
  (let [ch (.channel ctx)
        id (.id ch)
        cf (.closeFuture ch)
        out-sub (chan)]
    (try (.add channel-group ch)
         (async/sub out-pub id out-sub)
         (swap! clients assoc id
           {:ctx ctx
            :type :http ; changed in userEventTriggered
            :out-sub out-sub
            :addr (-> ch ^InetSocketAddress .remoteAddress HttpUtil/formatHostnameForHttp)})
         (when-not (put! in (->Connection id :http))
           (log/error "Unable to report connection because in chan is closed"))
         (.addListener cf
           (reify ChannelFutureListener
             (operationComplete [_ _]
               (when-not (put! in (->Connection id nil) (fn [_] (swap! clients dissoc id)))
                 (log/error "Unable to report disconnection because in chan is closed")))))
         (catch Exception e
           (log/error "Unable to register channel" ch e)
           (throw e)))))

; Responses

; after https://netty.io/4.1/xref/io/netty/example/http/websocketx/server/WebSocketIndexPageHandler.html
(defn respond!
  "Send HTTP response, and manage keep-alive and content-length.
   Fires channel read (provides backpressure) on success if keep-alive."
  [^ChannelHandlerContext ctx keep-alive? ^FullHttpResponse res]
  (log/debug "Responding in" (ess ctx) "with" (ess res) (when keep-alive? "and keep-alive"))
  (let [status (.status res)
        ; TODO do any other codes merit keep-alive?
        ok-or-continue? (contains? #{HttpResponseStatus/OK HttpResponseStatus/CONTINUE} status)
        keep-alive? (and keep-alive? ok-or-continue?)]
    (HttpUtil/setKeepAlive res keep-alive?)
    ; May need to review when enabling HttpContentEncoder etc. What about HTTP/2?
    (HttpUtil/setContentLength res (-> res .content .readableBytes))
    (let [cf (.writeAndFlush ctx res)]
      (if keep-alive?
        ; HTTP does actually permit pipelined half-duplex, where consecutive requests yield consecutive responses (on same channel) https://stackoverflow.com/questions/23419469
        ; The request-response backpressure from (.read ctx) here reduces the concurrency of request processing (i.e. doesn't start processing second request until first response is sent). Would need to profile with various workloads and core counts to work out if significant. Protects server at expense of single-channel client load time. Client is free to open another channel.
        (.addListener cf (reify ChannelFutureListener (operationComplete [_ _] (.read ctx))))
        (.addListener cf ChannelFutureListener/CLOSE)))))

(defn stream!
  "Send HTTP response streaming a file, and manage keep-alive and content-length.
   Application needs to supply :content-type header. Trailing headers not currently supported.
   Fires channel read (provides backpressure) on success if keep-alive."
  [^ChannelHandlerContext ctx keep-alive? ^HttpResponse res ^java.io.File file]
   ; after https://github.com/datskos/ring-netty-adapter/blob/master/src/ring/adapter/plumbing.clj and current docs
  (assert (.isFile file))
  (log/debug "Streaming" (.toString res))
  (let [status (.status res)
        ok? (= status HttpResponseStatus/OK)
        keep-alive? (and keep-alive? ok?)
        raf (RandomAccessFile. file "r")
        len (.length raf)
        ; connection seems to close prematurely when using DFR directly on file - because lazy open?
        region (DefaultFileRegion. (.getChannel raf) 0 len)
               #_(DefaultFileRegion. file 0 len) #_ (ChunkedFile. raf)]
        ; NB breaks if no os support for zero-copy; might not work with *netty's* ssl
    (HttpUtil/setKeepAlive res keep-alive?)
    (HttpUtil/setContentLength res len)
    (.writeAndFlush ctx res) ; initial line and header
    (let [cf (.writeAndFlush ctx region)] ; encoded into several HttpContents?
      (.addListener cf
        (reify ChannelFutureListener
          (operationComplete [_ _]
            (.close raf)
            ; Omission of LastHttpContent causes inability to reuse channel. Safari and wget just close channel and try again, firefox and chrome (ERR_INVALID_HTTP_RESPONSE) get upset and never finish loading page.
            (.writeAndFlush ctx (LastHttpContent/EMPTY_LAST_CONTENT))
            ; request-response backpressure!
            (when keep-alive? (.read ctx))
            (log/debug "Finished streaming" (ess res)
              "on" (ess ctx) (when-not keep-alive? ", about to close because not keep-alive"))))))))

(defn emergency!
  [ctx ^HttpResponseStatus status]
  (respond! ctx true (DefaultFullHttpResponse. HttpVersion/HTTP_1_1 status Unpooled/EMPTY_BUFFER)))

(defn responder
  "Asynchronously wait for application's (or `short-circuit`ed) response, or timeout.
   Send application's response to client with a backpressure-maintaning effect function.
   POST/PUT/PATCH requests are actually not fully read until application approves them with status 102;
   this status is intercepted here and causes channel read rather than being sent to client."
  [{:keys [clients handler-timeout] :as opts} id]
  (let [{:keys [^ChannelHandlerContext ctx ^HttpVersion protocol keep-alive?]} (get @clients id)
        ^HttpVersion protocol (or protocol HttpVersion/HTTP_1_1)
        closed (chan)
        _ (async/close! closed)
        out-sub (get-in @clients [id :out-sub] closed)] ; default to closed chan if out-sub closed
    (go
      (try
        (let [{:keys [status headers cookies content] :as res}
              ; Fetch application message published to this channel
              (alt! out-sub ([v] v) (async/timeout handler-timeout) ::timeout)
              ^HttpResponseStatus status (if (int? status) (HttpResponseStatus/valueOf status) status)]
          (case res
            ::timeout
            (do (log/error "Sent no http response on" (ess id) "because of out chan timeout")
                (emergency! ctx HttpResponseStatus/SERVICE_UNAVAILABLE))
            nil
            (do (log/warn "Sent no http response on" (ess id) "because out chan closed")
                (emergency! ctx HttpResponseStatus/SERVICE_UNAVAILABLE))
            (do
              #_(log/debug "Trying to send" (ess res))
              (case status 102 (.read ctx)
                (if (instance? java.io.File content)
                  ; TODO add support for other streaming sources (use protocols?)
                  ; Streaming:
                  (let [res (DefaultHttpResponse. protocol status)
                        hdrs (.headers res)]
                    (doseq [[k v] headers] (.set hdrs (-> k name str/lower-case) v))
                    (.set hdrs HttpHeaderNames/SET_COOKIE ^Iterable ; TODO expiry?
                      (mapv #(.encode ServerCookieEncoder/STRICT (first %) (second %)) cookies))
                    (stream! ctx keep-alive? res content))
                  ; Non-streaming:
                  (let [buf (condp #(%1 %2) content
                             string? (Unpooled/copiedBuffer ^String content CharsetUtil/UTF_8)
                             nil? Unpooled/EMPTY_BUFFER
                             bytes? (Unpooled/copiedBuffer ^bytes content))
                        res (DefaultFullHttpResponse. protocol status ^ByteBuf buf)
                        hdrs (.headers res)]
                    (doseq [[k v] headers] (.set hdrs (-> k name str/lower-case) v))
                    ; TODO need to support repeated headers (other than Set-Cookie ?)
                    (.set hdrs HttpHeaderNames/SET_COOKIE ^Iterable ; TODO expiry?
                      (mapv #(.encode ServerCookieEncoder/STRICT (first %) (second %)) cookies))
                    (respond! ctx keep-alive? res)))))))
        (catch Exception e
          (log/error "Error in http response handler" e)
          (emergency! ctx HttpResponseStatus/INTERNAL_SERVER_ERROR))))))

(defn short-circuit
  "Respond from this library, not application. Returns true if successful."
  [out channel ^HttpResponseStatus status]
  ; TODO html error pages?
  (or (async/put! out {:channel channel :status (.code status)})
      (log/warn "Unable to respond because out chan closed")))

; Requests

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

(defn read-chunkwise!
  "Read from HttpPostDecoder or fire channel read if partial data."
  [{:keys [clients in] :as opts} id]
  ; TODO work out how to indicate logged errors in while loop to user. Throw and catch? Cleanup? `code!`?
  (let [^HttpPostRequestDecoder decoder (get-in @clients [id :decoder])
        ^ChannelHandlerContext ctx (get-in @clients [id :ctx])]
    (try
      (while (.hasNext decoder)
        (log/debug "decoder has next")
        (when-let [^InterfaceHttpData data (.next decoder)]
          (when (= data (get-in @clients [id :partial])) (swap! clients update id dissoc :partial))
          (condp = (.getHttpDataType data)
            InterfaceHttpData$HttpDataType/Attribute
            (let [^io.netty.handler.codec.http.multipart.Attribute d data ; longhand to avoid clash
                  file? (-> d .isInMemory not)]
              (when-not (async/put! in
                          (->Attribute id
                            (.getName d) (.getCharset d)
                            file? (if file? (.getFile d) (.get d))))
                (log/error "Dropped incoming POST attribute because in chan is closed")))
            InterfaceHttpData$HttpDataType/FileUpload
            (let [^FileUpload d data file? (-> d .isInMemory not)]
              (if (.isCompleted d)
                (when-not (async/put! in
                            (->File id
                              (.getFilename d) (.getCharset d)
                              (.getContentType d) (.getContentTransferEncoding d)
                              file? (if file? (.getFile d) (.get d))))
                  (log/error "Dropped incoming POST file because in chan is closed"))
                (log/info "Dropped incoming POST file because upload incomplete")))
            (log/info "Dropped incoming POST data because unrecognised type"))))
      (when-let [^InterfaceHttpData data (.currentPartialHttpData decoder)]
        (log/debug "partial decoding")
        (.read ctx)
        (when-not (get-in @clients [id :partial])
          (swap! clients update id assoc :partial data)))
          ; TODO could do finer-grained logging/events etc
      (catch HttpPostRequestDecoder$EndOfDataDecoderException e
        (log/info (type e) (some->> e .getMessage (briefly 120)))))))

(defmulti ask-to!
  "Allows unacceptable requests to be stopped in flight, e.g. to prevent abusive POST.
   Application could customise by redefining method TBC.
   Returns boolean indicating whether `responder` should be called, i.e. to take from out chan.
   Out chan may have queued messages either from `short-circuit` or application."
  (fn [task opts ctx req] task))

(defmethod ask-to! ::continue
  [_ {:keys [out] :as opts} ^ChannelHandlerContext ctx ^HttpRequest req]
  (let [id (-> ctx .channel .id)
        protocol (.protocolVersion req)]
    (if (< 0 (.compareTo protocol HttpVersion/HTTP_1_1))
      (do (log/info "Wrong HTTP version" protocol)
          (short-circuit out id HttpResponseStatus/EXPECTATION_FAILED))
      (short-circuit out id HttpResponseStatus/CONTINUE))))

(defmethod ask-to! ::ws-upgrade ; FIXME only open ws door after http auth?
  ; Concept is "may the user on channel x (with channel-meta as noted)
  ; upgrade to websocket at path z?"
  [_ {:keys [ws-path out] :as opts} ^ChannelHandlerContext ctx ^HttpRequest req]
  (let [id (-> ctx .channel .id)
        p (.pipeline ctx)
        path (-> req .uri QueryStringDecoder. .path)]
    (if ws-path
      (if (= path ws-path)
        (do ; This is probably unnecessary (and noop) because HttpRequest has no content
            ; and so has no ByteBuf to pass back to pipeline!
            (ReferenceCountUtil/retain req)
            (.remove p "http-handler")
            (.fireChannelRead ctx req)
            false) ; don't respond from this handler
        (do (log/info "Wrong WS path" path)
            (short-circuit out id HttpResponseStatus/BAD_REQUEST)))
      (do (log/info "No WS configured" path)
          (short-circuit out id HttpResponseStatus/BAD_REQUEST)))))

(defmethod ask-to! ::receive
  [_ {:keys [in out] :as opts} ^ChannelHandlerContext ctx ^HttpRequest req]
  (let [id (-> ctx .channel .id)
        qsd (-> req .uri QueryStringDecoder.)
        {:keys [headers cookies]} (-> req .headers parse-headers)
        protocol (.protocolVersion req)
        method (-> req .method .toString str/lower-case keyword)]
    (or (async/put! in
          (->Request (-> ctx .channel .id) (.toString protocol)
            {:content-length
             (let [l (HttpUtil/getContentLength req (cast Long -1))]
               (when-not (neg? l) l))
             :charset (HttpUtil/getCharset req)
             :content-type (HttpUtil/getMimeType req)}
           method headers cookies (.uri req) (.path qsd) (.parameters qsd)))
        (do
          ; TODO Application can send 102 (to out chan, intercepted, not through to client)
          ; if happy to receive post/put/patch.
          ; This allows (.read ctx) in `responder` vs sending status code.
          ; Poor client support for latter! https://stackoverflow.com/questions/18367824
          (log/error "Dropped incoming http request because in chan is closed")
          (short-circuit out id HttpResponseStatus/SERVICE_UNAVAILABLE)))))

; Interface graph:
;   HttpObject
;     -> HttpContent
;          -> LastHttpContent
;               |-> FullHttpMessage
;     -> HttpMessage  |-> FullHttpRequest
;          -> HttpRequest

(defprotocol ReadableHttpObject
  (read! [this ctx opts] "Read channel and indicate whether to call responder."))

(extend-protocol ReadableHttpObject
  FullHttpRequest
  (read! [this ctx opts]
    (log/error "This library needs unaggregated HttpRequests."
      "Please remove HttpObjectAggregator from pipeline." this))

  HttpRequest
  (read! [this ctx {:keys [clients data-factory out max-content-length] :as opts}]
    (let [ch (.channel ctx)
          id (.id ch)
          protocol (.protocolVersion this)
          headers (.headers this)
          keep-alive? (HttpUtil/isKeepAlive this)
          method (-> this .method .toString str/lower-case keyword)
          decoder (case method
                    (:put :post :patch)
                    (try (HttpPostRequestDecoder. data-factory this)
                         (catch HttpPostRequestDecoder$ErrorDataDecoderException e
                           (log/warn "Error setting up POST decoder" e)
                           (short-circuit out id HttpResponseStatus/UNPROCESSABLE_ENTITY)))
                    ; Not accepting body for other methods
                    nil)]
      (swap! clients update id assoc :decoder decoder :protocol protocol :keep-alive? keep-alive?)
      (cond
        (-> this .decoderResult .isSuccess not)
        (do (log/warn (-> this .decoderResult .cause .getMessage))
            (short-circuit out id HttpResponseStatus/BAD_REQUEST))

        ; (cast Long 0) worked but (long 0) didn't! maybe...
        ; https://stackoverflow.com/questions/12586881
        ; FIXME vulnerable to reported < actual content length
        (> (HttpUtil/getContentLength this (cast Long 0))
           (get-in @clients [id :max-content-length] max-content-length))
        (do (log/info "Too large")
            (short-circuit out id HttpResponseStatus/REQUEST_ENTITY_TOO_LARGE))

        (.containsValue headers HttpHeaderNames/EXPECT HttpHeaderValues/CONTINUE true)
        (ask-to! ::continue opts ctx this)

        (.containsValue headers HttpHeaderNames/UPGRADE HttpHeaderValues/WEBSOCKET true)
        (ask-to! ::ws-upgrade opts ctx this)

        :else
        (ask-to! ::receive opts ctx this))))

  HttpContent
  ; Started with https://github.com/netty/netty/blob/master/example/src/main/java/io/netty/example/http/upload/HttpUploadServerHandler.java

  ; PUT and PATCH do work, but only if multipart/form-data (with ;boundary="blah") it seems.
  ; TODO try application/x-www-form-urlencoded ?
  ; Not enough to have content-type and data stream (tries to parse as attribute).
  ; This is probably ok; lots of benefit from HPRD's Mixed{Attribute,FileUpload} classes
  ; (in-memory, switches to disk after disk-threshold).

  ; FIXME work out exactly what HttpPostStandardRequestDecoder wants (non-multipart) wrt headers etc.

  (read! [this ctx {:keys [clients out in] :as opts}]
    (let [ch (.channel ctx)
          id (.id ch)
          decoder (get-in @clients [id :decoder])
          cleanup (fn [] (.destroy decoder) (swap! clients update id dissoc :decoder))]
      (if decoder
        (try (.offer decoder this)
             ; TODO could tally size of actual HttpContents and drop if abusive?
             ; (vs "stated" content length)
             (read-chunkwise! opts id)
             (when (instance? LastHttpContent this)
               (or (async/put! in
                     (->Trail id cleanup
                       (-> ^LastHttpContent this .trailingHeaders parse-headers :headers)))
                   (do (cleanup)
                       (log/error
                         "Couldn't deliver cleanup fn because in chan is closed. Cleaned up myself.")
                       (short-circuit out id HttpResponseStatus/SERVICE_UNAVAILABLE))))
             (catch HttpPostRequestDecoder$ErrorDataDecoderException e
               ; FIXME NPE with PUT x-url-encoded file... % curl ... -X PUT -d ...
               (log/warn "Error running POST decoder" (some->> e .getMessage (briefly 120)))
               (short-circuit out id HttpResponseStatus/UNPROCESSABLE_ENTITY))
             (catch IllegalStateException e
               (log/debug "Tried to write to destroyed decoder" e)))
        ; Ignore EmptyLastHttpContent following requests with methods which we don't allow to have body.
        #_(log/debug "Dropped" this "because no decoder. Does method support body?")))))

(defn ^ChannelHandler handler
  "Parse HTTP requests and forward to `in` with backpressure. Respond asynchronously from `out-sub`."
  ; TODO read about HTTP/2 https://developers.google.com/web/fundamentals/performance/http2
  [{:keys [clients disk-threshold] :as opts}]
  #_(log/debug "Starting http handler")
  (let [opts (assoc opts :data-factory (DefaultHttpDataFactory. ^long disk-threshold))]
    (set! (. DiskFileUpload deleteOnExitTemporaryFile) true) ; same as default
    (set! (. DiskFileUpload baseDirectory) nil) ; system temp directory
    (set! (. DiskAttribute deleteOnExitTemporaryFile) true)
    (set! (. DiskAttribute baseDirectory) nil)
    (proxy [SimpleChannelInboundHandler] [HttpObject]
      (channelRead0 [^ChannelHandlerContext ctx ^HttpObject obj]
        (log/debug "Received" (ess obj))
        (when (read! obj ctx opts)
          (responder opts (-> ctx .channel .id)))
        #_(log/debug "End of http/handler."))
      (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
        (let [ch (.channel ctx)
              id (.id ch)]
          (case (.getMessage cause)
            "Connection reset by peer" (log/info cause)
            (do (log/error "Error in http handler" cause)
                (emergency! ctx HttpResponseStatus/INTERNAL_SERVER_ERROR)))
          (some-> ^HttpPostRequestDecoder (get-in @clients [id :decoder]) .destroy))
        (.close ctx)))))