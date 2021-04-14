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
                                        HttpHeaderValues HttpExpectationFailedEvent FullHttpRequest)
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
(s/def ::content (s/or ::file ::file ::string string? ::bytes bytes? ::nil nil?))
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

; after https://netty.io/4.1/xref/io/netty/example/http/websocketx/server/WebSocketIndexPageHandler.html
(defn respond!
  "Send HTTP response, and manage keep-alive and Content-Length.
   Fires channel read (provides backpressure) on success if keep-alive."
  [^ChannelHandlerContext ctx keep-alive? ^FullHttpResponse res]
  (log/debug "Responding in" (ess ctx) "with" (ess res))
  (let [status (.status res)
        ; TODO do any other codes merit keep-alive?
        ok-or-continue? (contains? #{HttpResponseStatus/OK HttpResponseStatus/CONTINUE} status)
        keep-alive? (and keep-alive? ok-or-continue?)]
    (HttpUtil/setKeepAlive res keep-alive?)
    ; May need to review when enabling HttpContentEncoder etc. What about HTTP/2?
    (HttpUtil/setContentLength res (-> res .content .readableBytes))
    (let [cf (.writeAndFlush ctx res)]
      (if keep-alive?
        ; request-response backpressure!
        (.addListener cf (reify ChannelFutureListener (operationComplete [_ _] (.read ctx))))
        (.addListener cf ChannelFutureListener/CLOSE)))))

(defn code!
  "Send HTTP response with gives status and empty content using HTTP/1.1 or given version."
  ; TODO should probably pass through request's keep-alive...
  ([ctx status level msg] (code! ctx HttpVersion/HTTP_1_1 status level msg nil))
  ([ctx version status level msg] (code! ctx version status level msg nil))
  ([ctx ^HttpVersion v ^HttpResponseStatus s level msg e]
   (log/logp level msg e)
   (respond! ctx true (DefaultFullHttpResponse. v s Unpooled/EMPTY_BUFFER))))

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

(defn responder
  "Asynchronously wait for application's response, or timeout.
   Send application's response to client with a backpressure-maintaning effect function."
  [{:keys [clients handler-timeout] :as opts} id]
  (let [{:keys [^ChannelHandlerContext ctx ^HttpVersion protocol keep-alive?]} (get @clients id)
        protocol (or protocol HttpVersion/HTTP_1_1)
        closed (chan)
        _ (async/close! closed)
        out-sub (get-in @clients [id :out-sub] closed)] ; default to closed chan if out-sub closed
    (go
      (try
        (let [{:keys [status headers cookies content] :as res}
              ; Fetch application message published to this channel
              (alt! out-sub ([v] v) (async/timeout handler-timeout) ::timeout)]
          (case res
            ::timeout
            (code! ctx protocol HttpResponseStatus/SERVICE_UNAVAILABLE
              :error "Sent no http response because of out chan timeout")
            nil
            (code! ctx protocol HttpResponseStatus/SERVICE_UNAVAILABLE
              :warn "Sent no http response because out chan closed")
            (do
              ; TODO fire context read here for put/post/patch IF applications accepts e.g. 102?
              #_(log/debug "Trying to send" (ess res))
              (case status 102
                (.read ctx) ; and don't actually `respond!`
                (if (instance? java.io.File content)
                  ; TODO add support for other streaming sources (use protocols?)
                  ; Streaming:
                  (let [res (DefaultHttpResponse. protocol (HttpResponseStatus/valueOf status))
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
                        res (DefaultFullHttpResponse. ; needed protocol hinted to resolve?
                              protocol
                              (HttpResponseStatus/valueOf status)
                              ^ByteBuf buf)
                        hdrs (.headers res)]
                    (doseq [[k v] headers] (.set hdrs (-> k name str/lower-case) v))
                    ; TODO need to support repeated headers (other than Set-Cookie ?)
                    (.set hdrs HttpHeaderNames/SET_COOKIE ^Iterable ; TODO expiry?
                      (mapv #(.encode ServerCookieEncoder/STRICT (first %) (second %)) cookies))
                    (respond! ctx keep-alive? res)))))))
        (catch Exception e
          (code! ctx protocol HttpResponseStatus/INTERNAL_SERVER_ERROR
            :error "Error in http response handler" e))))))

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

(defn read-chunkwise
  "Reads from HttpPostDecoder and also fires channel read."
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

(defmulti ask-to ; FIXME all pretty procedural and not unit testable... rely on integration tests?
  "Deny HTTP request? Allows unacceptable requests to be stopped in flight, e.g. to prevent
   abusive POST. Application could customise by redefining method?"
  (fn [task opts ctx req] task))

(defmethod ask-to ::continue
  [_ opts ^ChannelHandlerContext ctx ^HttpRequest req]
  (let [protocol (.protocolVersion req)]
    (if (< 0 (.compareTo protocol HttpVersion/HTTP_1_1))
      (code! ctx protocol HttpResponseStatus/EXPECTATION_FAILED :info "Wrong HTTP version")
      (code! ctx protocol HttpResponseStatus/CONTINUE :debug "Please continue"))))

(defmethod ask-to ::ws-upgrade ; FIXME only open ws door after http auth?
  ; Concept is "may the user on channel x (with channel-meta as noted)
  ; upgrade to websocket at path z?"
  [_ {:keys [ws-path] :as opts} ^ChannelHandlerContext ctx ^HttpRequest req]
  (let [p (.pipeline ctx)
        protocol (.protocolVersion req)
        path (-> req .uri QueryStringDecoder. .path)]
    (if ws-path
      (if (= path ws-path)
        (do ; This is probably unnecessary (and noop) because HttpRequest has no content
            ; and so has no ByteBuf to pass back to pipeline!
            (ReferenceCountUtil/retain req)
            (.remove p "http-handler")
            (.fireChannelRead ctx req))
        (code! ctx protocol HttpResponseStatus/BAD_REQUEST
          :into (str "Wrong WS path " path)))
      (code! ctx protocol HttpResponseStatus/BAD_REQUEST
        :info (str "No WS configured " path)))))

(defmethod ask-to ::receive
  [_ {:keys [in] :as opts} ^ChannelHandlerContext ctx ^HttpRequest req]
  (let [qsd (-> req .uri QueryStringDecoder.)
        {:keys [headers cookies]} (-> req .headers parse-headers)
        protocol (.protocolVersion req)
        method (-> req .method .toString str/lower-case keyword)]
    (if-not (async/put! in
              (->Request (-> ctx .channel .id) (.toString protocol)
                {:content-length
                 (let [l (HttpUtil/getContentLength req (cast Long -1))]
                   (when-not (neg? l) l))
                 :charset (HttpUtil/getCharset req)
                 :content-type (HttpUtil/getMimeType req)}
               method headers cookies (.uri req) (.path qsd) (.parameters qsd)))
      ; TODO Application can send 102 (to out chan, not client) if happy to receive post/put/patch
      ; This causes (.read ctx) in `responder`... might not be kosher?
      ; https://stackoverflow.com/questions/18367824
      (code! ctx protocol HttpResponseStatus/SERVICE_UNAVAILABLE
        :error "Dropped incoming http request because in chan is closed"))))

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
  [{:keys [clients in disk-threshold max-content-length ws-path] :as opts}]
  #_(log/debug "Starting http handler")
  (let [data-factory (DefaultHttpDataFactory. ^long disk-threshold)]
    (set! (. DiskFileUpload deleteOnExitTemporaryFile) true) ; same as default
    (set! (. DiskFileUpload baseDirectory) nil) ; system temp directory
    (set! (. DiskAttribute deleteOnExitTemporaryFile) true)
    (set! (. DiskAttribute baseDirectory) nil)
    (proxy [SimpleChannelInboundHandler] [HttpObject]
      (channelRead0 [^ChannelHandlerContext ctx ^HttpObject obj]
        #_(log/debug "Received" (ess obj) "on" (ess (-> ctx .channel .id)))

        ; Provide default `permit` fn which app can override which looks at client map
        ; and decides whether to allow certain things

        ; TODO *** defence against unwelcome requests (esp large PUT/POST)
        ; Reject by default unless app says ok? Or dynamically set maximum-content-length?
        ; Broader app-controlled "permit" function?
        ; -> Could achieve by app unceremoniously evicting client on receipt of bad request?
        ; Or politely responding with HttpResponseStatus, but how to interrupt abusive upload?
        ; Would `respond!` turning off keep alive (if not OK or CONTINUE) terminate upload?
        ; TODO test

        (let [ch (.channel ctx)
              id (.id ch)]
          (when-let [^HttpRequest req (and (instance? HttpRequest obj) obj)]
            (when (instance? FullHttpRequest req)
              (log/warn "Unexpectedly received FullHttpRequest. POST decoder might not work."))
            (let [protocol (.protocolVersion req)
                  headers (.headers req)
                  keep-alive? (HttpUtil/isKeepAlive req)
                  decoder (try (HttpPostRequestDecoder. data-factory req)
                               (catch HttpPostRequestDecoder$ErrorDataDecoderException e
                                 (code! ctx protocol HttpResponseStatus/UNPROCESSABLE_ENTITY
                                   :warn "Error setting up POST decoder" e)))]
              ; Move this to ->Request meta if want application to be able to manipulate keep-alive
              (swap! clients update id assoc :decoder decoder :protocol protocol :keep-alive? keep-alive?)
              (cond
                (-> req .decoderResult .isSuccess not)
                (code! ctx HttpResponseStatus/BAD_REQUEST
                  :warn (-> req .decoderResult .cause .getMessage))

                (> (HttpUtil/getContentLength req (cast Long 0))
                   (get-in @clients [id :max-content-length] max-content-length))
                (code! ctx protocol HttpResponseStatus/REQUEST_ENTITY_TOO_LARGE :info "Too large")

                (.containsValue headers HttpHeaderNames/EXPECT HttpHeaderValues/CONTINUE true)
                (ask-to ::continue opts ctx req)

                (.containsValue headers HttpHeaderNames/UPGRADE HttpHeaderValues/WEBSOCKET true)
                (ask-to ::ws-upgrade opts ctx req)

                ; (cast Long 0) worked but (long 0) didn't! maybe...
                ; https://stackoverflow.com/questions/12586881/clojure-overloaded-method-resolution-for-longs
                ; FIXME may be vulnerable to reported < actual content length
                ; TODO tune max-content-length dep on app auth status? Could store in clients registry?
                (> (HttpUtil/getContentLength req (cast Long 0)) max-content-length)
                (code! ctx HttpResponseStatus/REQUEST_ENTITY_TOO_LARGE
                  :info "Too large")

                :else
                (ask-to ::receive opts ctx req))))

          ; Started with https://github.com/netty/netty/blob/master/example/src/main/java/io/netty/example/http/upload/HttpUploadServerHandler.java

          ; PUT and PATCH do work, but only if HttpHeaderValues/MULTIPART_FORM_DATA it seems
          ; Not enough to have content-type and data stream (tries to parse as attribute).
          ; This is probably ok; lots of benefit from HPRD's Mixed{Attribute,FileUpload} classes
          ; (in-memory, switches to disk after disk-threshold).

          (if-let [^HttpPostRequestDecoder decoder (get-in @clients [id :decoder])]
            (let [cleanup (fn [] (.destroy decoder) (swap! clients update id dissoc :decoder))]
              ; TODO what if some other obj comes through? Does pipeline guarantee?
              ; Should probably cleanup decoder and close chan.
              (when-let [^HttpContent con (and (instance? HttpContent obj) obj)]
                #_(log/debug "decoding")
                ; TODO could tally size of actual HttpContents and drop if abusive? (vs "stated" content length)
                (try (.offer decoder con)
                     (catch HttpPostRequestDecoder$ErrorDataDecoderException e
                       ; FIXME NPE with PUT x-url-encoded file... % curl ... -X PUT -d ...
                       (code! ctx HttpResponseStatus/UNPROCESSABLE_ENTITY
                         :warn (str "Error running POST decoder" (some->> e .getMessage (briefly 120))))))
                (read-chunkwise opts id)
                ; TODO what if doesn't come through? Does pipeline guarantee?
                (when-let [^LastHttpContent con (and (instance? LastHttpContent obj) obj)]
                  (when-not (async/put! in
                              (->Trail (-> ctx .channel .id) cleanup
                                       (-> con .trailingHeaders parse-headers :headers)))
                    (cleanup)
                    (log/error "Couldn't deliver cleanup fn because in chan is closed. Cleaned up myself."))
                  (responder opts id))))
            (responder opts id))
          #_(log/debug "End of http/handler.")))
      (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
        (let [ch (.channel ctx)
              id (.id ch)]
          (case (.getMessage cause)
            "Connection reset by peer" (log/info cause)
            (log/error "Error in http handler" cause))
          (some-> ^HttpPostRequestDecoder (get-in @clients [id :decoder]) .destroy))
        (.close ctx)))))