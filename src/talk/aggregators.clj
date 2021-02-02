(ns talk.aggregators
  (:import (io.netty.handler.codec MessageAggregator DecoderResult)
           (io.netty.handler.codec.http HttpObject HttpMessage HttpContent LastHttpContent FullHttpMessage HttpUtil HttpRequest HttpVersion HttpHeaderNames HttpHeaderValues HttpExpectationFailedEvent DefaultFullHttpResponse HttpResponseStatus FullHttpResponse HttpResponse HttpStatusClass HttpHeaders EmptyHttpHeaders FullHttpRequest DefaultFullHttpRequest HttpMethod)
           (io.netty.channel ChannelPipeline)
           (io.netty.buffer Unpooled ByteBuf)))


; Compensate for privacy or package-locality of HttpUtil methods
; Trying not to be clever

(defn isExpectHeaderValid [^HttpMessage msg]
  (and (instance? HttpRequest msg)
       (-> msg .protocolVersion (.compareTo HttpVersion/HTTP_1_1) (>= 0))))

(defn isUnsupportedExpectation [^HttpMessage message]
  (if-not (isExpectHeaderValid msg)
    false
    (let [expectValue (-> msg .headers (.get HttpHeaderNames/EXPECT))]
      (and (not (nil? expectValue))
           (not (-> HttpHeaderValues/CONTINUE .toString (.equalsIgnoreCase expectValue)))))))

(defn ignoreContentAfterContinueResponse [^Object msg]
  (if (instance? HttpResponse msg)
    (-> ^HttpResponse msg .status .codeClass (.equals HttpStatusClass/CLIENT_ERROR))
    false))

; HttpMessageUtil

(defn appendFullRequest [^StringBuilder buf ^FullHttpResponse res]
  ())

;

(defn response [^HttpResponseStatus s & {:as headers}]
  (let [r (DefaultFullHttpResponse. HttpVersion/HTTP_1_1 s Unpooled/EMPTY_BUFFER)]
    (doseq [[k v] headers] (-> r .headers (.set ^String k v)))
    r))

(defrecord AggregatedFullHttpMessage ; oh the pain
  [^HttpMessage message ^ByteBuf content ^HttpHeaders trailingHeaders]

  FullHttpMessage
  (^HttpHeaders trailingHeaders [_] ; https://clojure.atlassian.net/browse/CLJ-906
    (or trailingHeaders EmptyHttpHeaders/INSTANCE))
  ; setTrailingHeaders ... not override
  (^HttpVersion getProtocolVersion [_] (.protocolVersion message))
  (^HttpVersion protocolVersion [_] (.protocolVersion message))
  (^FullHttpMessage setProtocolVersion [this ^HttpVersion version]
    (.setProtocolVersion message version) this)
  (^HttpHeaders headers [_] (.headers message))
  (^DecoderResult decoderResult [_] (.decoderResult message))
  (^DecoderResult getDecoderResult [_] (.decoderResult message))
  (^nil setDecoderResult [_ ^DecoderResult result] (.setDecoderResult message result))
  (^ByteBuf content [_] content)
  (^int refCnt [_] (.refCnt content))
  (^FullHttpMessage retain [this] (.retain content) this)
  (^FullHttpMessage retain [this ^int increment] (.retain content increment) this)
  (^FullHttpMessage touch [this ^Object hint] (.touch content hint) this)
  (^FullHttpMessage touch [this] (.touch content) this)
  (^boolean release [_] (.release content))
  (^boolean release [_ ^int decrement] (.release content decrement))
  ; copy ... abstract
  ; duplicate
  ; retainedDuplicate))

  FullHttpRequest
  (^FullHttpRequest copy [this] (.replace this (.copy content)))
  (^FullHttpRequest duplicate [this] (.replace this (.duplicate content)))
  (^FullHttpRequest retainedDuplicate [this] (.replace this (.retainedDuplicate content)))
  (^FullHttpRequest replace [this ^ByteBuf content] ; will `content` resolve correctly?
    (doto (DefaultFullHttpRequest. (.protocolVersion this) (.method this) (.uri this)
            content (-> this .headers .copy) (.copy .trailingHeaders))
      (.setDecoderResult (.decoderResult this))))
  (^FullHttpRequest retain [this ^int increment] (.retain ^FullHttpMessage this increment) this)
  (^FullHttpRequest retain [this] (.retain ^FullHttpMessage this) this)
  (^FullHttpRequest touch [this] (.touch ^FullHttpMessage this) this)
  (^FullHttpRequest touch [this ^Object hint] (.touch ^FullHttpMessage this hint) this)
  (^FullHttpMessage setMethod [this ^HttpMethod method] (.setMethod ^HttpRequest message method) this)
  (^FullHttpRequest setUri [this ^String uri] (.setUri ^HttpRequest message uri) this)
  (^HttpMethod getMethod [_] (.method ^HttpRequest message))
  (^String getUri [_] (.uri ^HttpRequest message))
  (^HttpMethod method [this] (.getMethod this))
  (^String uri [this] (.getUri this))
  (^FullHttpRequest setProtocolVersion [this ^HttpVersion version]
    (.setProtocolVersion ^FullHttpMessage this version) this)
  (^String toString [this] ()))

(defn HttpObjectAggregator
  "Like netty's HttpObjectAggregator, but aggregates to disk if over threshold size."
  [{:keys [maxContentLength
           closeOnExpectationFailed
           diskThreshold]
    :or {closeOnExpectationFailed false}}]
  (let [EXPECTATION_FAILED (response HttpResponseStatus/EXPECTATION_FAILED
                             HttpHeaderNames/CONTENT_LENGTH 0)
        CONTINUE (response HttpResponseStatus/CONTINUE)
        TOO_LARGE (response HttpResponseStatus/REQUEST_ENTITY_TOO_LARGE
                    HttpHeaderNames/CONTENT_LENGTH 0)
        TOO_LARGE_CLOSE (response HttpResponseStatus/REQUEST_ENTITY_TOO_LARGE
                          HttpHeaderNames/CONTENT_LENGTH 0
                          HttpHeaderNames/CONNECTION HttpHeaderValues/CLOSE)]
    (proxy [MessageAggregator] [maxContentLength]
      (isStartMessage [^HttpObject msg] (instance? HttpMessage msg))
      (isContentMessage [^HttpObject msg] (instance? HttpContent msg))
      (isLastContentMessage [^HttpContent msg] (instance? LastHttpContent msg))
      (isAggregated [^HttpObject msg] (instance? FullHttpMessage msg))
      (isContentLengthInvalid [^HttpMessage start ^int maxContentLength]
        (try (> (HttpUtil/getContentLength start (long -1)) maxContentLength)
             (catch NumberFormatException _ false)))
      (newContinueResponse [^HttpMessage start ^int maxContentLength ^ChannelPipeline pipeline]
        (when-let [continueResponse
                   (if (isUnsupportedExpectation start)
                     (do (.fireUserEventTriggered pipeline HttpExpectationFailedEvent/INSTANCE)
                         (.retainedDuplicate EXPECTATION_FAILED))
                     (if (HttpUtil/is100ContinueExpected start)
                       (if (<= (HttpUtil/getContentLength start (long -1)) maxContentLength)
                         (.retainedDuplicate CONTINUE)
                         (do (.fireUserEventTriggered pipeline HttpExpectationFailedEvent/INSTANCE)
                             (.retainedDuplicate TOO_LARGE)))))]
          (-> start .headers (.remove HttpHeaderNames/EXPECT))
          continueResponse))
      (closeAfterContinueResponse [^Object msg]
        (and closeOnExpectationFailed (ignoreContentAfterContinueResponse msg)))
      (beginAggregation [^HttpMessage start ^ByteBuf content]
        (assert (not (instance? FullHttpMessage start)))
        (HttpUtil/setTransferEncodingChunked start false)))))

