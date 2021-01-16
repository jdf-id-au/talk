(ns talk.http
  (:require [clojure.tools.logging :as log])
  (:import (io.netty.buffer Unpooled)
           (io.netty.channel ChannelHandler SimpleChannelInboundHandler ChannelHandlerContext)
           (io.netty.handler.codec.http HttpRequest HttpContent HttpUtil
                                        DefaultFullHttpResponse
                                        HttpResponseStatus HttpVersion FullHttpRequest)))

(defn ^ChannelHandler handler
  []
  (proxy [SimpleChannelInboundHandler] [FullHttpRequest]
    ; TODO work through https://github.com/netty/netty/blob/e5951d46fc89db507ba7d2968d2ede26378f0b04/example/src/main/java/io/netty/example/http/snoop/HttpSnoopServerHandler.java
    (channelRead0 [^ChannelHandlerContext ctx ^FullHttpRequest req]
      (log/info req)
      (.write ctx (DefaultFullHttpResponse. HttpVersion/HTTP_1_1 HttpResponseStatus/OK Unpooled/EMPTY_BUFFER)))
    (exceptionCaught [^ChannelHandlerContext ctx ^Throwable cause]
      (log/error cause)
      (.close ctx))))