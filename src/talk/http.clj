(ns talk.http
  "Parse http requests and forward to `in` with backpressure.
   Respond asynchronously from `out-sub` or timeout."
  (:require [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go go-loop chan <!! >!! <! >!
                                                  put! close! alt! alt!!]]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [talk.server :as server :refer [Aggregator ChannelInboundMessageHandler
                                            accept]]
            [clojure.java.io :as io])
  (:import (io.netty.buffer Unpooled ByteBuf)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler
                             ChannelHandlerContext ChannelFutureListener ChannelOption
                             DefaultFileRegion)
           (io.netty.handler.codec.http HttpUtil
                                        DefaultFullHttpResponse DefaultHttpResponse
                                        HttpResponseStatus
                                        FullHttpRequest FullHttpResponse
                                        HttpHeaderNames QueryStringDecoder HttpMethod HttpResponse
                                        DiskHttpObjectAggregator$AggregatedFullHttpRequest
                                        HttpRequest HttpContent LastHttpContent)
           (io.netty.util CharsetUtil ReferenceCountUtil)
           (io.netty.handler.codec.http.cookie ServerCookieDecoder ServerCookieEncoder Cookie)
           (io.netty.handler.codec.http.multipart HttpPostRequestDecoder
                                                  InterfaceHttpData
                                                  DefaultHttpDataFactory
                                                  FileUpload Attribute
                                                  DiskFileUpload DiskAttribute
                                                  MixedFileUpload MixedAttribute)
           (java.io File RandomAccessFile)
           (java.net URLConnection)
           (io.netty.handler.codec MixedData)
           (java.nio.file Files)
           (java.nio.channels FileChannel)))

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

(def upload-handler
  "Handle HTTP POST/PUT/PATCH data. Does not apply charset automatically.
   NB data >16kB will be stored in tempfiles, which will be lost on JVM shutdown.
   Must cleanup after response sent; this will also remove tempfiles."
  ; NB limited by needing to fit in memory because of HttpObjectAggregator,
  ; which I failed to reimplement to have disk backing.
  ; after https://gist.github.com/breznik/6215834
  ; TODO protect against file upload abuse somehow
  ; (partly protected by max-content-length and backpressure)
  (let [data-factory (DefaultHttpDataFactory.)] ; memory if <16kB, else disk
    (set! (. DiskFileUpload deleteOnExitTemporaryFile) true) ; same as default
    (set! (. DiskFileUpload baseDirectory) nil) ; system temp directory
    (set! (. DiskAttribute deleteOnExitTemporaryFile) true)
    (set! (. DiskAttribute baseDirectory) nil)
    (fn [^DiskHttpObjectAggregator$AggregatedFullHttpRequest req]
      (condp contains? (.method req)
        #{HttpMethod/POST}
        ; FIXME might be much easier just to support PUT and PATCH for huge files?
        (let [decoder (HttpPostRequestDecoder. data-factory req)] ; FIXME adapt to use MixedData
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
        #{HttpMethod/PUT HttpMethod/PATCH}
        (let [charset (HttpUtil/getCharset req)
              base {:charset (-> charset .toString keyword)}
              #_#_u (doto (.createAttribute data-factory req "put or post")
                      (.setContent (-> req .retain .content)))]
          {:cleanup #(.cleanRequestHttpData data-factory req)
           :data #_(vector (if (.isInMemory u)
                             (assoc base :value (.get u))
                             (assoc base :file (.getFile u))))
           ; from struggles with DiskHttpObjectAggregator:
                 (vector (if (.isInMemory req)
                           (assoc base :value (.get ^MixedData (.retain req)))
                           (assoc base :file (.getFile ^MixedData (.retain req)))))})
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
      (server/track-channel ctx admin)
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
              basics {:ch id :type ::request
                      :method (-> method .toString str/lower-case keyword)
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
              {:keys [data cleanup] :or {cleanup (constantly nil)}} (upload-handler req)
              request-map
              (cond-> (assoc basics :headers headers :cookies cookies)
                data (assoc :data data)
                ; Shouldn't really have body for GET, DELETE, TRACE, OPTIONS, HEAD
                #_#_(not data) (assoc :content (some-> req .content
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

(defn parse-req
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
     :keep-alive? (HttpUtil/isKeepAlive req)}))
    ; not accommodating body for GET, DELETE, TRACE, OPTIONS or HEAD
    ; (not data) (assoc :content (some-> req .content (.toString (HttpUtil/getCharset req))))))

(defn put [bc port-kw msg]
  (or (put! (get bc port-kw)
            (case port-kw :chunks [bc msg] :messages msg)
            #(when % (.read (:ctx bc))))
      (log/error "Dropped incoming http message because" port-kw "channel is closed." msg)))
  ; TODO throw something?


(defrecord Disk [meta file stream]
  Aggregator
  (accept [so-far msg bc]))

(defrecord Memory [meta content]
  Aggregator
  (accept [so-far msg bc]))

; "Don't extend in lib if don't own both protocol and target type."
;(extend-type nil
;  Aggregator
;  (accept [this msg bc]
;    (if (some->> bc ::content-length (> (:size-threshold bc)))
;      (->Memory (atom msg)))))

(defn store [so-far msg bc]
  {:pre [(nil? so-far)]}
  (if (some-> bc ::content-length (> (:size-threshold bc)))
    (->Memory msg nil)
    (let [tf (Files/createTempFile "talk" "http-agg" [])
          fc (FileChannel/open tf [])]
      (->Disk msg tf fc))))

(extend-protocol ChannelInboundMessageHandler
  ; Interface graph:
  ;   HttpContent
  ;     -> LastHttpContent
  ;          |-> FullHttpMessage
  ;   HttpMessage  |-> FullHttpRequest
  ;     -> HttpRequest

  FullHttpRequest
  ; TODO confirm that (hopefully) this is "more specific" than LastHttpContent
  ; and so can be used to identify non-chunked messages.
  (channelRead0 [msg bc] (put bc :messages msg))
  (offer [_ _ _])

  HttpRequest
  (channelRead0 [msg bc] (put bc :chunks msg))
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
                    [::release-failed]))))))))

  HttpContent
  (channelRead0 [msg bc] (put bc :chunks msg))
  (offer [msg so-far bc]
    (if-not so-far
      [::first-missing]
      [::ok (accept so-far msg bc)]))

  LastHttpContent
  (channelRead0 [msg bc] (put bc :chunks msg))
  (offer [msg so-far bc]
    ; TODO deal with nil so-far?
    [:last]))
