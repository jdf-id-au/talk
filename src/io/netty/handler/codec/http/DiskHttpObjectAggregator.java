/*
 * Copyright 2012 The Netty Project
 * Adaptation copyright 2021 Jeremy Field <jeremy.field@gmail.com>
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.MixedData;
import io.netty.handler.codec.DiskMessageAggregator;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.MixedAttribute;
import io.netty.util.Attribute;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.EXPECT;
import static io.netty.handler.codec.http.HttpUtil.getContentLength;

/**
 * Like {@link HttpObjectAggregator} but streams large objects to disk.
 */
public class DiskHttpObjectAggregator
        extends DiskMessageAggregator<HttpObject, HttpMessage, HttpContent, FullHttpMessage> {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(HttpObjectAggregator.class);
    private static final FullHttpResponse CONTINUE =
            new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE, Unpooled.EMPTY_BUFFER);
    private static final FullHttpResponse EXPECTATION_FAILED = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.EXPECTATION_FAILED, Unpooled.EMPTY_BUFFER);
    private static final FullHttpResponse TOO_LARGE_CLOSE = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, Unpooled.EMPTY_BUFFER);
    private static final FullHttpResponse TOO_LARGE = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_1, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, Unpooled.EMPTY_BUFFER);

    static {
        EXPECTATION_FAILED.headers().set(CONTENT_LENGTH, 0);
        TOO_LARGE.headers().set(CONTENT_LENGTH, 0);

        TOO_LARGE_CLOSE.headers().set(CONTENT_LENGTH, 0);
        TOO_LARGE_CLOSE.headers().set(CONNECTION, HttpHeaderValues.CLOSE);
    }

    private final boolean closeOnExpectationFailed;

    /**
     * Creates a new instance.
     * @param maxContentLength the maximum length of the aggregated content in bytes.
     * If the length of the aggregated content exceeds this value,
     * {@link #handleOversizedMessage(ChannelHandlerContext, HttpMessage)} will be called.
     */
    public DiskHttpObjectAggregator(long maxContentLength) {
        this(maxContentLength, false);
    }

    /**
     * Creates a new instance.
     * @param maxContentLength the maximum length of the aggregated content in bytes.
     * If the length of the aggregated content exceeds this value,
     * {@link #handleOversizedMessage(ChannelHandlerContext, HttpMessage)} will be called.
     * @param closeOnExpectationFailed If a 100-continue response is detected but the content length is too large
     * then {@code true} means close the connection. otherwise the connection will remain open and data will be
     * consumed and discarded until the next request is received.
     */
    public DiskHttpObjectAggregator(long maxContentLength, boolean closeOnExpectationFailed) {
        super(maxContentLength);
        this.closeOnExpectationFailed = closeOnExpectationFailed;
    }

    @Override
    protected boolean isStartMessage(HttpObject msg) throws Exception {
        return msg instanceof HttpMessage;
    }

    @Override
    protected boolean isContentMessage(HttpObject msg) throws Exception {
        return msg instanceof HttpContent;
    }

    @Override
    protected boolean isLastContentMessage(HttpContent msg) throws Exception {
        return msg instanceof LastHttpContent;
    }

    @Override
    protected boolean isAggregated(HttpObject msg) throws Exception {
        return msg instanceof FullHttpMessage;
    }

    @Override
    protected boolean isContentLengthInvalid(HttpMessage start, long maxContentLength) {
        try {
            return getContentLength(start, -1L) > maxContentLength;
        } catch (final NumberFormatException e) {
            return false;
        }
    }

    private static Object continueResponse(HttpMessage start, long maxContentLength, ChannelPipeline pipeline) {
        if (HttpUtil.isUnsupportedExpectation(start)) {
            // if the request contains an unsupported expectation, we return 417
            pipeline.fireUserEventTriggered(HttpExpectationFailedEvent.INSTANCE);
            return EXPECTATION_FAILED.retainedDuplicate();
        } else if (HttpUtil.is100ContinueExpected(start)) {
            // if the request contains 100-continue but the content-length is too large, we return 413
            if (getContentLength(start, -1L) <= maxContentLength) {
                return CONTINUE.retainedDuplicate();
            }
            pipeline.fireUserEventTriggered(HttpExpectationFailedEvent.INSTANCE);
            return TOO_LARGE.retainedDuplicate();
        }

        return null;
    }

    @Override
    protected Object newContinueResponse(HttpMessage start, long maxContentLength, ChannelPipeline pipeline) {
        Object response = continueResponse(start, maxContentLength, pipeline);
        // we're going to respond based on the request expectation so there's no
        // need to propagate the expectation further.
        if (response != null) {
            start.headers().remove(EXPECT);
        }
        return response;
    }

    @Override
    protected boolean closeAfterContinueResponse(Object msg) {
        return closeOnExpectationFailed && ignoreContentAfterContinueResponse(msg);
    }

    @Override
    protected boolean ignoreContentAfterContinueResponse(Object msg) {
        if (msg instanceof HttpResponse) {
            final HttpResponse httpResponse = (HttpResponse) msg;
            return httpResponse.status().codeClass().equals(HttpStatusClass.CLIENT_ERROR);
        }
        return false;
    }

    @Override
    protected AggregatedFullHttpMessage beginAggregation(HttpMessage start, ByteBuf content) throws Exception {
        assert !(start instanceof FullHttpMessage);

        HttpUtil.setTransferEncodingChunked(start, false);

        AggregatedFullHttpMessage ret;
        if (start instanceof HttpRequest) {
            ret = new AggregatedFullHttpRequest((HttpRequest) start, content, null);
        } else if (start instanceof HttpResponse) {
            ret = new AggregatedFullHttpResponse((HttpResponse) start, content, null);
        } else {
            throw new Error();
        }
        return ret;
    }

    @Override
    protected void aggregate(FullHttpMessage aggregated, HttpContent content) throws Exception {
        if (content instanceof LastHttpContent) {
            // Merge trailing headers into the message.
            ((AggregatedFullHttpMessage) aggregated).setTrailingHeaders(((LastHttpContent) content).trailingHeaders());
        }
    }

    @Override
    protected void finishAggregation(FullHttpMessage aggregated) throws Exception {
        // Set the 'Content-Length' header. If one isn't already set.
        // This is important as HEAD responses will use a 'Content-Length' header which
        // does not match the actual body, but the number of bytes that would be
        // transmitted if a GET would have been used.
        //
        // See rfc2616 14.13 Content-Length
        if (!HttpUtil.isContentLengthSet(aggregated)) {
            aggregated.headers().set(
                    CONTENT_LENGTH,
                    String.valueOf(aggregated.content().readableBytes()));
        }
    }

    @Override
    protected void handleOversizedMessage(final ChannelHandlerContext ctx, HttpMessage oversized) throws Exception {
        if (oversized instanceof HttpRequest) {
            // send back a 413 and close the connection

            // If the client started to send data already, close because it's impossible to recover.
            // If keep-alive is off and 'Expect: 100-continue' is missing, no need to leave the connection open.
            if (oversized instanceof FullHttpMessage ||
                !HttpUtil.is100ContinueExpected(oversized) && !HttpUtil.isKeepAlive(oversized)) {
                ChannelFuture future = ctx.writeAndFlush(TOO_LARGE_CLOSE.retainedDuplicate());
                future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            logger.debug("Failed to send a 413 Request Entity Too Large.", future.cause());
                        }
                        ctx.close();
                    }
                });
            } else {
                ctx.writeAndFlush(TOO_LARGE.retainedDuplicate()).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (!future.isSuccess()) {
                            logger.debug("Failed to send a 413 Request Entity Too Large.", future.cause());
                            ctx.close();
                        }
                    }
                });
            }
        } else if (oversized instanceof HttpResponse) {
            ctx.close();
            throw new TooLongFrameException("Response entity too large: " + oversized);
        } else {
            throw new IllegalStateException();
        }
    }

    private abstract static class AggregatedFullHttpMessage implements FullHttpMessage, MixedData {
        protected final HttpMessage message;
        private final MixedAttribute storage;
        private HttpHeaders trailingHeaders;

        AggregatedFullHttpMessage(HttpMessage message, ByteBuf content, HttpHeaders trailingHeaders) {
            this.message = message;
            this.storage = new MixedAttribute("aggregator", DefaultHttpDataFactory.MINSIZE);
            try {
                //storage.setContent(content); // (potentially) misleadingly does setCompleted()
                storage.addContent(content, false);
            } catch (IOException e) {
                logger.error("Unable to create aggregator", e);
            }
            this.trailingHeaders = trailingHeaders;
        }

        public void addContent(ByteBuf buffer, boolean last) throws IOException {
            this.storage.addContent(buffer, last); // misleadingly setCompleted() in AMHD
            logger.info(
                " buffer " + buffer.readableBytes() +
                " last " + last +
                " defined length " + this.storage.definedLength() +
                " storage length " + this.storage.length() + // seemingly 8KB chunks
                " isInMemory " + this.storage.isInMemory() +
                " isCompleted " + this.storage.isCompleted() + // misleadingly? true when initially in memory
                " getFile " + (this.storage.isInMemory() ? "n/a" : this.storage.getFile()) +
                " file length " + (this.storage.isInMemory() ? "n/a" :this.storage.getFile().length() )
            );
        }

        public byte[] get() throws IOException {
            return this.storage.get();
        }

        public boolean isInMemory() {
            return this.storage.isInMemory();
        }

        public File getFile() throws IOException {
            return this.storage.getFile();
        }

        @Override
        public HttpHeaders trailingHeaders() {
            HttpHeaders trailingHeaders = this.trailingHeaders;
            if (trailingHeaders == null) {
                return EmptyHttpHeaders.INSTANCE;
            } else {
                return trailingHeaders;
            }
        }

        void setTrailingHeaders(HttpHeaders trailingHeaders) {
            this.trailingHeaders = trailingHeaders;
        }

        @Override
        public HttpVersion getProtocolVersion() {
            return message.protocolVersion();
        }

        @Override
        public HttpVersion protocolVersion() {
            return message.protocolVersion();
        }

        @Override
        public FullHttpMessage setProtocolVersion(HttpVersion version) {
            message.setProtocolVersion(version);
            return this;
        }

        @Override
        public HttpHeaders headers() {
            return message.headers();
        }

        @Override
        public DecoderResult decoderResult() {
            return message.decoderResult();
        }

        @Override
        public DecoderResult getDecoderResult() {
            return message.decoderResult();
        }

        @Override
        public void setDecoderResult(DecoderResult result) {
            message.setDecoderResult(result);
        }

        @Override
        public ByteBuf content() {
            return this.storage.content();
        }

        @Override
        public int refCnt() {
            return this.storage.refCnt();
        }

        @Override
        public FullHttpMessage retain() {
            this.storage.retain();
            return this;
        }

        @Override
        public FullHttpMessage retain(int increment) {
            this.storage.retain(increment);
            return this;
        }

        @Override
        public FullHttpMessage touch(Object hint) {
            this.storage.touch(hint);
            return this;
        }

        @Override
        public FullHttpMessage touch() {
            this.storage.touch();
            return this;
        }

        @Override
        public boolean release() {
            return this.storage.release();
        }

        @Override
        public boolean release(int decrement) {
            return this.storage.release(decrement);
        }

        @Override
        public abstract FullHttpMessage copy();

        @Override
        public abstract FullHttpMessage duplicate();

        @Override
        public abstract FullHttpMessage retainedDuplicate();
    }

    private static final class AggregatedFullHttpRequest extends AggregatedFullHttpMessage implements FullHttpRequest {

        AggregatedFullHttpRequest(HttpRequest request, ByteBuf content, HttpHeaders trailingHeaders) {
            super(request, content, trailingHeaders);
        }

        @Override
        public FullHttpRequest copy() {
            return replace(content().copy());
        }

        @Override
        public FullHttpRequest duplicate() {
            return replace(content().duplicate());
        }

        @Override
        public FullHttpRequest retainedDuplicate() {
            return replace(content().retainedDuplicate());
        }

        @Override
        public FullHttpRequest replace(ByteBuf content) {
            DefaultFullHttpRequest dup = new DefaultFullHttpRequest(protocolVersion(), method(), uri(), content,
                    headers().copy(), trailingHeaders().copy());
            dup.setDecoderResult(decoderResult());
            return dup;
        }

        @Override
        public FullHttpRequest retain(int increment) {
            super.retain(increment);
            return this;
        }

        @Override
        public FullHttpRequest retain() {
            super.retain();
            return this;
        }

        @Override
        public FullHttpRequest touch() {
            super.touch();
            return this;
        }

        @Override
        public FullHttpRequest touch(Object hint) {
            super.touch(hint);
            return this;
        }

        @Override
        public FullHttpRequest setMethod(HttpMethod method) {
            ((HttpRequest) message).setMethod(method);
            return this;
        }

        @Override
        public FullHttpRequest setUri(String uri) {
            ((HttpRequest) message).setUri(uri);
            return this;
        }

        @Override
        public HttpMethod getMethod() {
            return ((HttpRequest) message).method();
        }

        @Override
        public String getUri() {
            return ((HttpRequest) message).uri();
        }

        @Override
        public HttpMethod method() {
            return getMethod();
        }

        @Override
        public String uri() {
            return getUri();
        }

        @Override
        public FullHttpRequest setProtocolVersion(HttpVersion version) {
            super.setProtocolVersion(version);
            return this;
        }

        @Override
        public String toString() {
            return HttpMessageUtil.appendFullRequest(new StringBuilder(256), this).toString();
        }
    }

    private static final class AggregatedFullHttpResponse extends AggregatedFullHttpMessage
            implements FullHttpResponse {

        AggregatedFullHttpResponse(HttpResponse message, ByteBuf content, HttpHeaders trailingHeaders) {
            super(message, content, trailingHeaders);
        }

        @Override
        public FullHttpResponse copy() {
            return replace(content().copy());
        }

        @Override
        public FullHttpResponse duplicate() {
            return replace(content().duplicate());
        }

        @Override
        public FullHttpResponse retainedDuplicate() {
            return replace(content().retainedDuplicate());
        }

        @Override
        public FullHttpResponse replace(ByteBuf content) {
            DefaultFullHttpResponse dup = new DefaultFullHttpResponse(getProtocolVersion(), getStatus(), content,
                    headers().copy(), trailingHeaders().copy());
            dup.setDecoderResult(decoderResult());
            return dup;
        }

        @Override
        public FullHttpResponse setStatus(HttpResponseStatus status) {
            ((HttpResponse) message).setStatus(status);
            return this;
        }

        @Override
        public HttpResponseStatus getStatus() {
            return ((HttpResponse) message).status();
        }

        @Override
        public HttpResponseStatus status() {
            return getStatus();
        }

        @Override
        public FullHttpResponse setProtocolVersion(HttpVersion version) {
            super.setProtocolVersion(version);
            return this;
        }

        @Override
        public FullHttpResponse retain(int increment) {
            super.retain(increment);
            return this;
        }

        @Override
        public FullHttpResponse retain() {
            super.retain();
            return this;
        }

        @Override
        public FullHttpResponse touch(Object hint) {
            super.touch(hint);
            return this;
        }

        @Override
        public FullHttpResponse touch() {
            super.touch();
            return this;
        }

        @Override
        public String toString() {
            return HttpMessageUtil.appendFullResponse(new StringBuilder(256), this).toString();
        }
    }
}
