/**
 * Copyright (c) 2015 CodisLabs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.codis.nedis.handler;

import io.codis.nedis.exception.RedisResponseException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.net.ProtocolException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 * @author zhangduo
 */
public class RedisResponseDecoder extends ByteToMessageDecoder {

    public static final Object NULL_REPLY = new Object();

    private void setReaderIndex(ByteBuf in, int index) {
        in.readerIndex(index == -1 ? in.writerIndex() : index + 1);
    }

    private String decodeString(ByteBuf in) throws ProtocolException {
        final StringBuilder buffer = new StringBuilder();
        final MutableBoolean reachCRLF = new MutableBoolean(false);
        setReaderIndex(in, in.forEachByte(new ByteBufProcessor() {

            @Override
            public boolean process(byte value) throws Exception {
                if (value == '\n') {
                    if ((byte) buffer.charAt(buffer.length() - 1) != '\r') {
                        throw new ProtocolException("Response is not ended by CRLF");
                    } else {
                        buffer.setLength(buffer.length() - 1);
                        reachCRLF.setTrue();
                        return false;
                    }
                } else {
                    buffer.append((char) value);
                    return true;
                }
            }
        }));
        return reachCRLF.booleanValue() ? buffer.toString() : null;
    }

    private int toDigit(byte b) {
        return b - '0';
    }

    private Long decodeLong(ByteBuf in) throws ProtocolException {
        byte sign = in.readByte();
        final MutableLong l;
        boolean negative;
        if (sign == '-') {
            negative = true;
            l = new MutableLong(0);
        } else {
            negative = false;
            l = new MutableLong(toDigit(sign));
        }
        final MutableBoolean reachCR = new MutableBoolean(false);
        setReaderIndex(in, in.forEachByte(new ByteBufProcessor() {

            @Override
            public boolean process(byte value) throws Exception {
                if (value == '\r') {
                    reachCR.setTrue();
                    return false;
                } else {
                    if (value >= '0' && value <= '9') {
                        l.setValue(l.longValue() * 10 + toDigit(value));
                    } else {
                        throw new ProtocolException("Response is not ended by CRLF");
                    }
                    return true;
                }
            }
        }));
        if (!reachCR.booleanValue()) {
            return null;
        }
        if (!in.isReadable()) {
            return null;
        }
        if (in.readByte() != '\n') {
            throw new ProtocolException("Response is not ended by CRLF");
        }
        return negative ? -l.longValue() : l.longValue();
    }

    private boolean decode(ByteBuf in, List<Object> out, Object nullValue) throws Exception {
        if (in.readableBytes() < 2) {
            return false;
        }
        byte b = in.readByte();
        switch (b) {
            case '+': {
                String reply = decodeString(in);
                if (reply == null) {
                    return false;
                }
                out.add(reply);
                return true;
            }
            case '-': {
                String reply = decodeString(in);
                if (reply == null) {
                    return false;
                }
                out.add(new RedisResponseException(reply));
                return true;
            }
            case ':': {
                Long reply = decodeLong(in);
                if (reply == null) {
                    return false;
                }
                out.add(reply);
                return true;
            }
            case '$': {
                Long numBytes = decodeLong(in);
                if (numBytes == null) {
                    return false;
                }
                if (numBytes.intValue() == -1) {
                    out.add(nullValue);
                    return true;
                }
                if (in.readableBytes() < numBytes.intValue() + 2) {
                    return false;
                }
                if (in.getByte(in.readerIndex() + numBytes.intValue()) != '\r'
                        || in.getByte(in.readerIndex() + numBytes.intValue() + 1) != '\n') {
                    throw new ProtocolException("Response is not ended by CRLF");
                }
                byte[] reply = new byte[numBytes.intValue()];
                in.readBytes(reply);
                // skip CRLF
                in.skipBytes(2);
                out.add(reply);
                return true;
            }
            case '*': {
                Long numReplies = decodeLong(in);
                if (numReplies == null) {
                    return false;
                }
                if (numReplies.intValue() == -1) {
                    out.add(nullValue);
                    return true;
                }
                List<Object> replies = new ArrayList<>();
                for (int i = 0; i < numReplies.intValue(); i++) {
                    if (!decode(in, replies, null)) {
                        return false;
                    }
                }
                out.add(replies);
                return true;
            }
            default:
                throw new ProtocolException("Unknown leading char: " + (char) b);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        in.markReaderIndex();
        if (!decode(in, out, NULL_REPLY)) {
            in.resetReaderIndex();
        }
    }

}
