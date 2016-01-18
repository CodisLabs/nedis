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

import io.netty.util.concurrent.Future;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author zhangduo
 * @see http://redis.io/commands#list
 */
public interface ListsCommands extends BlockingListsCommands {

    Future<byte[]> lindex(byte[] key, long index);

    public enum LIST_POSITION {
        BEFORE, AFTER;
        public final byte[] raw;

        private LIST_POSITION() {
            raw = name().getBytes(StandardCharsets.UTF_8);
        }
    }

    Future<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value);

    Future<Long> llen(byte[] key);

    Future<byte[]> lpop(byte[] key);

    Future<Long> lpush(byte[] key, byte[]... values);

    Future<Long> lpushx(byte[] key, byte[] value);

    Future<List<byte[]>> lrange(byte[] key, long startInclusive, long stopInclusive);

    Future<Long> lrem(byte[] key, long count, byte[] value);

    Future<byte[]> lset(byte[] key, long index, byte[] value);

    Future<Void> ltrim(byte[] key, long startInclusive, long stopInclusive);

    Future<byte[]> rpop(byte[] key);

    Future<byte[]> rpoplpush(byte[] src, byte[] dst);

    Future<Long> rpush(byte[] key, byte[]... values);

    Future<Long> rpushx(byte[] key, byte[] value);
}
