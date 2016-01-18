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
package io.codis.nedis.protocol;

import static io.codis.nedis.util.NedisUtils.toBytes;

/**
 * @author Apache9
 * @see http://redis.io/commands/scan
 */
public class ScanParams {

    private static final byte[] START_CURSOR = toBytes("0");

    private byte[] cursor = START_CURSOR;

    private byte[] pattern;

    private long count;

    public byte[] cursor() {
        return cursor;
    }

    public ScanParams cursor(byte[] cursor) {
        this.cursor = cursor != null ? cursor : START_CURSOR;
        return this;
    }

    public byte[] match() {
        return pattern;
    }

    public ScanParams match(byte[] pattern) {
        this.pattern = pattern;
        return this;
    }

    public long count() {
        return count;
    }

    public ScanParams count(long count) {
        this.count = count;
        return this;
    }
}
