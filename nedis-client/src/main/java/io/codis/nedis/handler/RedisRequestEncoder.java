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

import static io.codis.nedis.util.NedisUtils.toBytes;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * @author Apache9
 */
public class RedisRequestEncoder {

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

    private static int paramCountSize(int paramCount) {
        // * + paramCount + CRLF
        return 1 + stringSize(paramCount) + 2;
    }

    private static int paramSize(byte[] param) {
        // $ + paramLength + CRLF + param + CRLF
        return 1 + stringSize(param.length) + 2 + param.length + 2;
    }

    private static int serializedSize(byte[] cmd, byte[][] params, byte[]... otherParams) {
        int size = paramCountSize(1 + params.length + otherParams.length) + paramSize(cmd);
        for (byte[] param: params) {
            size += paramSize(param);
        }
        for (byte[] param: otherParams) {
            size += paramSize(param);
        }
        return size;
    }

    private static int serializedSize(byte[] cmd, List<byte[]> params) {
        int size = paramCountSize(1 + params.size()) + paramSize(cmd);
        for (byte[] param: params) {
            size += paramSize(param);
        }
        return size;
    }

    private static void writeParamCount(ByteBuf buf, int paramCount) {
        buf.writeByte('*').writeBytes(toBytes(paramCount)).writeBytes(CRLF);
    }

    private static void writeParam(ByteBuf buf, byte[] param) {
        buf.writeByte('$').writeBytes(toBytes(param.length)).writeBytes(CRLF).writeBytes(param)
                .writeBytes(CRLF);
    }

    public static ByteBuf encode(ByteBufAllocator alloc, byte[] cmd, byte[]... params) {
        int serializedSize = serializedSize(cmd, params);
        ByteBuf buf = alloc.buffer(serializedSize, serializedSize);
        writeParamCount(buf, params.length + 1);
        writeParam(buf, cmd);
        for (byte[] param: params) {
            writeParam(buf, param);
        }
        return buf;
    }

    public static ByteBuf encode(ByteBufAllocator alloc, byte[] cmd, byte[][] headParams,
            byte[]... tailParams) {
        int serializedSize = serializedSize(cmd, headParams, tailParams);
        ByteBuf buf = alloc.buffer(serializedSize, serializedSize);
        writeParamCount(buf, headParams.length + tailParams.length + 1);
        writeParam(buf, cmd);
        for (byte[] param: headParams) {
            writeParam(buf, param);
        }
        for (byte[] param: tailParams) {
            writeParam(buf, param);
        }
        return buf;
    }

    public static ByteBuf encodeReverse(ByteBufAllocator alloc, byte[] cmd, byte[][] tailParams,
            byte[]... headParams) {
        int serializedSize = serializedSize(cmd, tailParams, headParams);
        ByteBuf buf = alloc.buffer(serializedSize, serializedSize);
        writeParamCount(buf, headParams.length + tailParams.length + 1);
        writeParam(buf, cmd);
        for (byte[] param: headParams) {
            writeParam(buf, param);
        }
        for (byte[] param: tailParams) {
            writeParam(buf, param);
        }
        return buf;
    }

    public static ByteBuf encode(ByteBufAllocator alloc, byte[] cmd, List<byte[]> params) {
        int serializedSize = serializedSize(cmd, params);
        ByteBuf buf = alloc.buffer(serializedSize, serializedSize);
        writeParamCount(buf, params.size() + 1);
        writeParam(buf, cmd);
        for (byte[] param: params) {
            writeParam(buf, param);
        }
        return buf;
    }
}
