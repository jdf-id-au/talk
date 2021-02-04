package io.netty.handler.codec;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

/**
 * Minimum version of {@link io.netty.handler.codec.http.multipart.HttpData}
 * as used my {@link io.netty.handler.codec.http.multipart.MixedAttribute},
 * except for use from {@link DiskMessageAggregator}.
 */
public interface MixedData {
    void addContent(ByteBuf buffer, boolean last) throws IOException;
}