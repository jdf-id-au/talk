/*
 * Copyright 2013 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.http.websocketx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DiskMessageAggregator;
import io.netty.handler.codec.MixedData;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import io.netty.handler.codec.http.multipart.MixedAttribute;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Handler that aggregate fragmented WebSocketFrame's.
 *
 * Be aware if PING/PONG/CLOSE frames are send in the middle of a fragmented {@link WebSocketFrame} they will
 * just get forwarded to the next handler in the pipeline.
 */
public class DiskWebSocketFrameAggregator
        extends DiskMessageAggregator<WebSocketFrame, WebSocketFrame, ContinuationWebSocketFrame, WebSocketFrame> {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(DiskWebSocketFrameAggregator.class);
    /**
     * Creates a new instance
     *
     * @param maxContentLength If the size of the aggregated frame exceeds this value,
     *                         a {@link TooLongFrameException} is thrown.
     */
    public DiskWebSocketFrameAggregator(int maxContentLength) {
        super(maxContentLength);
    }

    @Override
    protected boolean isStartMessage(WebSocketFrame msg) throws Exception {
        return msg instanceof TextWebSocketFrame || msg instanceof BinaryWebSocketFrame;
    }

    @Override
    protected boolean isContentMessage(WebSocketFrame msg) throws Exception {
        return msg instanceof ContinuationWebSocketFrame;
    }

    @Override
    protected boolean isLastContentMessage(ContinuationWebSocketFrame msg) throws Exception {
        return isContentMessage(msg) && msg.isFinalFragment();
    }

    @Override
    protected boolean isAggregated(WebSocketFrame msg) throws Exception {
        if (msg.isFinalFragment()) {
            return !isContentMessage(msg);
        }

        return !isStartMessage(msg) && !isContentMessage(msg);
    }

    @Override
    protected boolean isContentLengthInvalid(WebSocketFrame start, long maxContentLength) {
        return false;
    }

    @Override
    protected Object newContinueResponse(WebSocketFrame start, long maxContentLength, ChannelPipeline pipeline) {
        return null;
    }

    @Override
    protected boolean closeAfterContinueResponse(Object msg) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean ignoreContentAfterContinueResponse(Object msg) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected AggregatedWebSocketFrame beginAggregation(WebSocketFrame start, ByteBuf content) throws Exception {
        assert !(start instanceof AggregatedWebSocketFrame);
        if (start instanceof TextWebSocketFrame) {
            return new AggregatedTextWebSocketFrame(true, start.rsv(), content);
        } else if (start instanceof BinaryWebSocketFrame) {
            return new AggregatedBinaryWebSocketFrame(true, start.rsv(), content);
        } else {
            throw new Error();
        }
    }

    private abstract static class AggregatedWebSocketFrame extends WebSocketFrame implements MixedData, ByteBufHolder {
        private final MixedAttribute storage;

        AggregatedWebSocketFrame(boolean finalFragment, int rsv, ByteBuf content) {
            super(finalFragment, rsv, content);
            this.storage = new MixedAttribute("ws-agg", DefaultHttpDataFactory.MINSIZE);
            try {
                storage.setContent(content);
            } catch (IOException e) {
                logger.error("Unable to create aggregator", e);
            }
        }

        public void addContent(ByteBuf buffer, boolean last) throws IOException {
            // arrgh
            logger.debug(" this " + this.refCnt() +
                    " buffer " + buffer.refCnt() +
                    " storage " + storage.refCnt());
            this.storage.addContent(buffer, last);
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
        public ByteBuf content() {
            return this.storage.content();
        }

        @Override
        public int refCnt() {
            return this.storage.refCnt();
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
        public abstract WebSocketFrame copy();
        
        @Override
        public abstract WebSocketFrame duplicate();
        
        @Override
        public abstract WebSocketFrame retainedDuplicate();
    }
    
    private static final class AggregatedTextWebSocketFrame extends AggregatedWebSocketFrame {
        
        AggregatedTextWebSocketFrame(boolean finalFragment, int rsv, ByteBuf content) {
            super(finalFragment, rsv, content);
        }
        
        @Override
        public TextWebSocketFrame copy() {
            return replace(content().copy());
        }
        
        @Override
        public TextWebSocketFrame duplicate() {
            return replace(content().duplicate());
        }
        
        @Override
        public TextWebSocketFrame retainedDuplicate() {
            return replace(content().retainedDuplicate());
        }
        
        @Override
        public TextWebSocketFrame replace(ByteBuf content) {
            return new TextWebSocketFrame(this.isFinalFragment(), this.rsv(), content);
        }
        
        @Override
        public AggregatedTextWebSocketFrame retain(int increment) {
            super.retain(increment);
            return this;
        }
        
        @Override
        public AggregatedTextWebSocketFrame retain() {
            super.retain();
            return this;
        }
        
        @Override
        public AggregatedTextWebSocketFrame touch() {
            super.touch();
            return this;
        }
    }

    private static final class AggregatedBinaryWebSocketFrame extends AggregatedWebSocketFrame {

        AggregatedBinaryWebSocketFrame(boolean finalFragment, int rsv, ByteBuf content) {
            super(finalFragment, rsv, content);
        }

        @Override
        public BinaryWebSocketFrame copy() {
            return replace(content().copy());
        }

        @Override
        public BinaryWebSocketFrame duplicate() {
            return replace(content().duplicate());
        }

        @Override
        public BinaryWebSocketFrame retainedDuplicate() {
            return replace(content().retainedDuplicate());
        }

        @Override
        public BinaryWebSocketFrame replace(ByteBuf content) {
            return new BinaryWebSocketFrame(this.isFinalFragment(), this.rsv(), content);
        }

        @Override
        public AggregatedBinaryWebSocketFrame retain(int increment) {
            super.retain(increment);
            return this;
        }

        @Override
        public AggregatedBinaryWebSocketFrame retain() {
            super.retain();
            return this;
        }

        @Override
        public AggregatedBinaryWebSocketFrame touch() {
            super.touch();
            return this;
        }
    }

}
