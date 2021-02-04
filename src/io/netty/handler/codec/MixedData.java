package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * Like multipart.HttpData, but less.
 */
public interface MixedData {
    void addContent(ByteBuf buffer, boolean last) throws IOException;
}
