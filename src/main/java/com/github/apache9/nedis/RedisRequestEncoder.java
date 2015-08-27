/**
 * @(#)RequestEncoder.java, 2015-8-27. 
 *
 * Copyright (c) 2015, Wandou Labs and/or its affiliates. All rights reserved.
 * WANDOU LABS PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
package com.github.apache9.nedis;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author zhangduo
 */
public class RedisRequestEncoder extends MessageToByteEncoder<byte[][]> {

    private static final byte[] CRLF = new byte[] {
        '\r', '\n'
    };

    private final static int[] SIZE_TABLE = {
        9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, Integer.MAX_VALUE
    };

    // Requires positive x
    private static int stringSize(int x) {
        for (int i = 0;; i++) {
            if (x <= SIZE_TABLE[i]) {
                return i + 1;
            }
        }
    }

    private byte[] toBytes(int value) {
        return Integer.toString(value).getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, byte[][] msg, ByteBuf out) throws Exception {
        out.writeByte('*').writeBytes(toBytes(msg.length)).writeBytes(CRLF);
        for (byte[] param: msg) {
            out.writeByte('$').writeBytes(toBytes(param.length)).writeBytes(CRLF).writeBytes(param)
                    .writeBytes(CRLF);
        }
    }

    private int serializedSize(byte[][] msg) {
        int size = 1 + stringSize(msg.length) + 2;
        for (byte[] param: msg) {
            size += 1 + stringSize(param.length) + 2 + param.length + 2;
        }
        return size;
    }

    @Override
    protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, byte[][] msg, boolean preferDirect)
            throws Exception {
        int size = serializedSize(msg);
        return preferDirect ? ctx.alloc().ioBuffer(size) : ctx.alloc().heapBuffer(size);
    }

}
