(ns talk.http
  (:require [clojure.tools.logging :as log])
  (:import (io.netty.buffer Unpooled ByteBufUtil)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler ChannelHandlerContext ChannelFutureListener)
           (io.netty.handler.codec.http HttpRequest HttpContent HttpUtil
                                        DefaultFullHttpResponse
                                        HttpResponseStatus HttpVersion FullHttpRequest FullHttpResponse HttpMethod HttpHeaderNames)
           (io.netty.util CharsetUtil)))

; after https://netty.io/4.1/xref/io/netty/example/http/websocketx/server/WebSocketIndexPageHandler.html
(defn respond! [^ChannelHandlerContext ctx ^FullHttpRequest req ^FullHttpResponse res]
  (let [status (.status res)
        ok? (= (.code status) 200)
        keep-alive? (and (HttpUtil/isKeepAlive req) ok?)]
    #_(when-not ok?
        (ByteBufUtil/writeUtf8 (.content res) (.toString status))
        (HttpUtil/setContentLength res (-> res .content .readableBytes)))
    (HttpUtil/setKeepAlive res keep-alive?)
    (let [cf (.writeAndFlush ctx res)]
      (when-not keep-alive? (.addListener cf ChannelFutureListener/CLOSE)))))

(defn ^ChannelHandler handler
  [path]
  (proxy [SimpleChannelInboundHandler] [FullHttpRequest]
    ; TODO work through https://github.com/netty/netty/blob/e5951d46fc89db507ba7d2968d2ede26378f0b04/example/src/main/java/io/netty/example/http/snoop/HttpSnoopServerHandler.java
    (channelRead0 [^ChannelHandlerContext ctx ^FullHttpRequest req]
      #_(log/info req)
      (cond
        (-> req .decoderResult .isSuccess not)
        (respond! ctx req (DefaultFullHttpResponse. (.protocolVersion req)
                            HttpResponseStatus/BAD_REQUEST Unpooled/EMPTY_BUFFER))

        (-> req .method (= HttpMethod/GET) not)
        (respond! ctx req (DefaultFullHttpResponse. (.protocolVersion req)
                            HttpResponseStatus/FORBIDDEN Unpooled/EMPTY_BUFFER))

        (-> req .uri (= "/hmm"))
        (let [content (Unpooled/copiedBuffer "hello" CharsetUtil/UTF_8)
              res (DefaultFullHttpResponse. (.protocolVersion req)
                    HttpResponseStatus/OK content)
              _ (-> res .headers (.set HttpHeaderNames/CONTENT_TYPE "text/html; charset=UTF-8"))
              _ (HttpUtil/setContentLength res (.readableBytes content))]
          (respond! ctx req res))

        :else
        (respond! ctx req (DefaultFullHttpResponse. (.protocolVersion req)
                            HttpResponseStatus/NOT_FOUND Unpooled/EMPTY_BUFFER))))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
      (log/error "Error in http handler" cause)
      (.close ctx))))