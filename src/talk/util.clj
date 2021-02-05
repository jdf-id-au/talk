(ns talk.util
  "Cheesy reimplementations of some access-restricted netty methods."
  (:import (io.netty.handler.codec.http
             HttpMessage HttpRequest HttpVersion
             HttpHeaderNames HttpHeaderValues
             HttpExpectationFailedEvent
             DefaultHttpResponse HttpResponseStatus DefaultFullHttpResponse HttpStatusClass HttpUtil HttpResponse)
           (io.netty.channel ChannelPipeline)
           (io.netty.buffer Unpooled ByteBuf ByteBufHolder)))

(defn dfhr [^HttpResponseStatus s & {:as headers}]
  (let [r (DefaultFullHttpResponse. HttpVersion/HTTP_1_1 s Unpooled/EMPTY_BUFFER)]
    (doseq [[k v] headers] (-> r .headers (.set ^String k v)))
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


; HttpUtil

(defn isExpectHeaderValid [^HttpMessage message]
  (and (instance? HttpRequest message)
    (-> message .protocolVersion (.compareTo HttpVersion/HTTP_1_1) (>= 0))))

(defn isUnsupportedExpectation [^HttpMessage message]
  (if-not (isExpectHeaderValid message)
    false
    (let [expectValue (-> message .headers (.get HttpHeaderNames/EXPECT))]
      (and (not (nil? expectValue))
           (not (-> HttpHeaderValues/CONTINUE .toString (.equalsIgnoreCase expectValue)))))))

; HttpObjectAggregator

(defn continueResponse [^HttpMessage start maxContentLength ^ChannelPipeline pipeline]
  (if (isUnsupportedExpectation start)
    (do (.fireUserEventTriggered pipeline HttpExpectationFailedEvent/INSTANCE)
        (.retainedDuplicate (:EXPECTATION_FAILED response)))
    (when (HttpUtil/is100ContinueExpected start)
      (when (<= (HttpUtil/getContentLength start (long -1)) maxContentLength)
        (.retainedDuplicate (:CONTINUE response)))
      (.fireUserEventTriggered pipeline HttpExpectationFailedEvent/INSTANCE)
      (.retainedDuplicate (:TOO_LARGE response)))))

(defn ignoreContentAfterContinueResponse [msg]
  (if (instance? HttpResponse msg)
    (-> ^HttpResponse msg .status .codeClass (.equals HttpStatusClass/CLIENT_ERROR))
    false))
