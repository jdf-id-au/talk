(ns talk.http
  "Parse http requests and forward to `in` with backpressure.
   Respond asynchronously from `out-sub` or timeout."
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            #_[talk.util :as util]
            #_[talk.server :as server :refer [ChannelInboundMessageHandler offer
                                              Aggregator accept]])
  (:import (io.netty.buffer Unpooled ByteBuf)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler
                             ChannelHandlerContext ChannelFutureListener ChannelOption
                             DefaultFileRegion)
           (io.netty.handler.codec.http HttpUtil
                                        DefaultFullHttpResponse DefaultHttpResponse
                                        HttpResponseStatus
                                        FullHttpRequest FullHttpResponse
                                        HttpHeaderNames QueryStringDecoder HttpMethod HttpResponse
                                        HttpRequest HttpContent LastHttpContent HttpObject HttpVersion)
           (io.netty.util CharsetUtil ReferenceCountUtil)
           (io.netty.handler.codec.http.cookie ServerCookieDecoder ServerCookieEncoder Cookie)
           (io.netty.handler.codec.http.multipart HttpPostRequestDecoder
                                                  InterfaceHttpData
                                                  DefaultHttpDataFactory
                                                  FileUpload Attribute
                                                  DiskFileUpload DiskAttribute
                                                  MixedFileUpload MixedAttribute)
           (java.io File RandomAccessFile)
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
                  410 411 412 413 414 415 416 417
                      421 422 423 424 425 426     428 429
                      431
                  500 501 502 503 504 505 506 507
                  510 511})

; TODO could enforce HTTP semantics in spec (e.g. (s/and ... checking-fn)
; (Some like CORS preflight might need to be stateful and therefore elsewhere...)
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
           :addr (-> ch ^InetSocketAddress .remoteAddress .getAddress .toString))
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
  [^ChannelHandlerContext ctx keep-alive? ^HttpResponse res ^File file]
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
    (.writeAndFlush ctx res) ; initial line and header
    (let [cf (.writeAndFlush ctx region)] ; encoded into several HttpContents?
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))

(defn responder [{:keys [in clients handler-timeout state] :as opts}
                 ^HttpVersion protocol-version keep-alive?
                 request-map cleanup]
  (let [^ChannelHandlerContext ctx (:ctx @state)
        id (-> ctx .channel .id)
        out-sub (get-in @clients [id :out-sub])]
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
                                  Unpooled/EMPTY_BUFFER)))))))
(def upload-handler
  "Handle HTTP POST/PUT/PATCH data. Does not apply charset automatically.
   NB data >16kB will be stored in tempfiles, which will be lost on JVM shutdown.
   Must cleanup after response sent; this will also remove tempfiles."

  ; NB limited by needing to fit in memory because of HttpObjectAggregator,
  ; after https://gist.github.com/breznik/6215834 (seems like source for netty example below?)

  ; TODO *** try without HOA (although confirm if supports PUT/PATCH - should? looks at content-type?)
  ; https://github.com/netty/netty/blob/master/example/src/main/java/io/netty/example/http/upload/HttpUploadServerHandler.java

  ; TODO protect against file upload abuse somehow
  ; (partly protected by max-content-length and backpressure)
  (let [data-factory (DefaultHttpDataFactory.)] ; memory if <16kB, else disk
    (set! (. DiskFileUpload deleteOnExitTemporaryFile) true) ; same as default
    (set! (. DiskFileUpload baseDirectory) nil) ; system temp directory
    (set! (. DiskAttribute deleteOnExitTemporaryFile) true)
    (set! (. DiskAttribute baseDirectory) nil)
    (fn [^HttpRequest req]
      (condp contains? (.method req)
        #{HttpMethod/POST}
        (let [decoder (HttpPostRequestDecoder. data-factory req)]
          {:cleanup #(.destroy decoder)
           :data
           (some->>
             (for [^InterfaceHttpData d (.getBodyHttpDatas decoder)
                   :let [base {:name (.getName d)}]]
               (condp instance? d
                 ; Only if application/x-www-form-urlencoded ?
                 Attribute
                 (let [a ^MixedAttribute d
                       base (assoc base :charset (some-> a .getCharset .toString keyword))]
                   (if (.isInMemory a)
                     ; NB puts onus on application to apply charset
                     ; e.g. (slurp value :encoding "UTF-8")
                     (assoc base :value (.get a))
                     (assoc base :file (.getFile a))))
                 FileUpload
                 (let [f ^MixedFileUpload d
                       base (assoc base :charset (some-> f .getCharset .toString keyword)
                                        :client-filename (.getFilename f)
                                        :content-type (.getContentType f)
                                        :content-transfer-encoding (.getContentTransferEncoding f))]
                   (if (.isInMemory f)
                     (assoc base :value (.get f))
                     (assoc base :file (.getFile f))))
                 (log/info "Unsupported http body data type "
                   (assoc base :type (.getHttpDataType d)))))
             seq (into []))})
        ; FIXME HPDR??
        #{HttpMethod/PUT HttpMethod/PATCH}
        (let [charset (HttpUtil/getCharset req)
              base {:charset (-> charset .toString keyword)}
              u (doto (.createAttribute data-factory req "put or post")
                  (.setContent (-> req .retain .content)))]
          {:cleanup #(.cleanRequestHttpData data-factory req)
           :data (vector (if (.isInMemory u)
                           (assoc base :value (.get u))
                           (assoc base :file (.getFile u))))})
        ; else
        nil))))

(defn ^ChannelHandler handler
  "Parse HTTP requests and forward to `in` with backpressure. Respond asynchronously from `out-sub`."
  ; TODO read about HTTP/2 https://developers.google.com/web/fundamentals/performance/http2
  [{:keys [clients state] :as opts}]
  (proxy [SimpleChannelInboundHandler] [HttpObject]
    (channelActive [^ChannelHandlerContext ctx]
      ; TODO is this safe? Doesn't given channel keep same context instance? Set by pipeline in initChannel?
      (swap! state assoc :ctx ctx)
      (track-channel opts)
      (-> ctx .channel .config
            ; facilitate backpressure on subsequent reads; requires .read see branches below
        (-> (.setAutoRead false)
            ; May be needed for response from outside netty event loop:
            ; https://stackoverflow.com/a/48128514/780743
            (.setOption ChannelOption/ALLOW_HALF_CLOSURE true)))
      (.read ctx)) ; first read
    (channelInactive [^ChannelHandlerContext ctx])
      ; TODO decoder.cleanFiles()
    (channelRead0 [^ChannelHandlerContext ctx ^HttpObject obj]
      ; Would be nice to dispatch using protocol, but I don't own netty types so "shouldn't in lib code".
      (when-let [^HttpRequest msg (and (instance? HttpRequest obj) obj)]
        (if (-> msg .decoderResult .isSuccess)
          (let [ch (.channel ctx)
                id (.id ch)
                method (.method msg)
                qsd (-> msg .uri QueryStringDecoder.)
                keep-alive? (HttpUtil/isKeepAlive msg)
                protocol-version (.protocolVersion msg) ; get info from req before released (necessary?)
                basics {:ch id :type ::request
                        :method (-> method .toString str/lower-case keyword)
                        :path (.path qsd)
                        :query (.parameters qsd)
                        :protocol (-> msg .protocolVersion .toString)}
                {:keys [headers cookies]}
                (some->> msg .headers .iteratorAsString iterator-seq
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
                {:keys [data cleanup] :or {cleanup (constantly nil)}} (upload-handler msg)
                request-map
                (cond-> (assoc basics :headers headers :cookies cookies)
                  data (assoc :data data)
                  ; Shouldn't really have body for GET, DELETE, TRACE, OPTIONS, HEAD
                  #_#_(not data) (assoc :content (some-> msg .content
                                                   (.toString (HttpUtil/getCharset msg)))))]
            (responder opts protocol-version keep-alive? request-map cleanup))
          (do (log/info "Decoder failure" (-> msg .decoderResult .cause))
              (respond! ctx false (DefaultFullHttpResponse.
                                    (.protocolVersion msg)
                                    HttpResponseStatus/BAD_REQUEST
                                    Unpooled/EMPTY_BUFFER))))))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
      (log/error "Error in http handler" cause)
      (.close ctx))))

#_(defn parse-req
    "Parse out nearly everything from HttpRequest except body."
    [{:keys [ctx] :as bc} ^HttpRequest req]
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
                        ; TODO could look up max-age, etc...
                        [(.name c) (.value c)]))
                    (update m :headers
                      (fn [hs]
                        (if-let [old (get hs lck)]
                          (if (vector? old)
                            (conj old v)
                            [old v])
                          (assoc hs lck v)))))))
              {}))]
      {:ch (-> ctx .channel .id)
       :type ::request ; TODO needed?
       :method (-> req .method .toString str/lower-case keyword)
       :path (.path qsd)
       :query (.parameters qsd)
       :protocol (-> req .protocolVersion .toString)
       :headers headers
       :cookies cookies
       :keep-alive? (HttpUtil/isKeepAlive req)
       :content-length (and (HttpUtil/isContentLengthSet req) (HttpUtil/getContentLength req))}))
    ; not accommodating body for GET, DELETE, TRACE, OPTIONS or HEAD
    ; (not data) (assoc :content (some-> req .content (.toString (HttpUtil/getCharset req))))))

#_(defn store
    "Put processed msg in appropriate record."
    [so-far msg bc]
    {:pre [(nil? so-far)]}
    (if (some-> bc ::content-length (> (:size-threshold bc)))
      ; Over threshold
      (apply ->Disk msg (tempfile "http-agg"))
      ; Under threshold or not stated
      (->Memory msg nil)))

#_(defn put
    "Put message on specified chan."
    [bc chan-key msg]
    (or (put! (get bc chan-key) (case chan-key :chunks [bc msg] :messages msg)
          #(when % (.read (:ctx bc))))
      (log/error "Dropped incoming http message because" chan-key "channel is closed." msg)))
      ; TODO throw something?

#_(extend-protocol ChannelInboundMessageHandler
    ; Interface graph (dispatch on "most specific"):
    ;   HttpObject
    ;     -> HttpContent
    ;          -> LastHttpContent
    ;               |-> FullHttpMessage
    ;     -> HttpMessage  |-> FullHttpRequest
    ;          -> HttpRequest

    ; https://clojure.org/reference/protocols
    ; "if one interface is derived from the other, the more derived is used,
    ;  else which one is used is unspecified"

    FullHttpRequest ; FIXME need to deal with content if present
    (channelRead0 [msg bc] (put bc :messages (parse-req bc msg)))
    (offer [_ _ _])

    ; channelRead0 of HttpRequest, HttpContent and LastHttpContent
    ; are derived from MessageAggregator -> HttpObjectAggregator.
    ; Java's access modifiers are really annoying.

    HttpRequest
    (channelRead0 [msg {:keys [^ChannelHandlerContext ctx state
                               ^long max-content-length] :as bc}]
      ; TODO *** hammer through impl, just for http, then contemplate best way to incl ws/refactor
      ; continue response
      (if-let [continueResponse (util/continueResponse msg max-content-length (.pipeline ctx))]
        (let [listener (reify ChannelFutureListener
                         (operationComplete [_ cf]
                           (when-not (.isSuccess cf) (.fireExceptionCaught ctx (.cause cf)))))
              closeOnExpectationFailed false ; config
              hOM (util/ignoreContentAfterContinueResponse continueResponse)
              closeAfterWrite (and closeOnExpectationFailed hOM)
              future (-> ctx (.writeAndFlush continueResponse) (.addListener listener))]
          (swap! state assoc
            :handlingOversizeMessage hOM
            :continueResponseWriteListener listener)
          (cond closeAfterWrite (.addListener future ChannelFutureListener/CLOSE)
                hOM ::decoder-returns))) ; FIXME annoying translating to functional flow control
      ; NB adapting from MessageAggregator decode which is called by same class' channelRead
      ; which releases the message but that might be right my way
      ; `out` in decode seems to be the messages it passes through to the next handler, presumably plus the messages it has aggregated


      (put bc :chunks msg))
    (offer [msg so-far bc]
      (if so-far
        [::not-first]
        (if-not (-> msg .decoderResult .isSuccess)
          [::decoder-fail msg]
          (let [length (and (HttpUtil/isContentLengthSet msg) (HttpUtil/getContentLength msg))]
            (if (> length (:max-content-length bc))
              [::too-long]
              (let [{:keys [method] :as parsed} (parse-req bc msg)]
                (case method
                  :post nil ; TODO delegate aggregation to HttpPostRequestDecoder somehow
                  (if (ReferenceCountUtil/release msg)
                      ; ::content-length redundant with the corresponding header but better-parsed
                      [::start (store so-far parsed (assoc bc ::content-length length))]
                      [::release-fail]))))))))

    HttpContent
    (channelRead0 [msg bc]
      (put bc :chunks msg))
    (offer [msg so-far bc]
      (if-not so-far
        [::first-missing]
        ; TODO stream content either to memory (how exactly?) or disk;
        ; check if size grows to exceed size-threshold and release netty resource
        [::ok (accept so-far msg bc)]))

    LastHttpContent
    (channelRead0 [msg bc] (put bc :chunks msg))
    (offer [msg so-far bc]
      ; TODO deal with nil so-far?
      (if-not so-far
        [::prev-missing]
        (let [] ; TODO process msg as above and release netty resource
          [::last (accept so-far)]))))
