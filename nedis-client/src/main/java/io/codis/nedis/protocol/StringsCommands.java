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

import java.util.List;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 * @see http://redis.io/commands#string
 */
public interface StringsCommands {

    Future<Long> append(byte[] key, byte[] value);

    Future<Long> bitcount(byte[] key);

    Future<Long> bitcount(byte[] key, long startInclusive, long endInclusive);

    Future<Long> bitop(BitOp op, byte[] dst, byte[]... keys);

    Future<Long> bitpos(byte[] key, boolean bit);

    Future<Long> bitpos(byte[] key, boolean bit, long startInclusive);

    Future<Long> bitpos(byte[] key, boolean bit, long startInclusive, long endInclusive);

    Future<Long> decr(byte[] key);

    Future<Long> decrby(byte[] key, long delta);

    Future<byte[]> get(byte[] key);

    Future<Boolean> getbit(byte[] key, long offset);

    Future<byte[]> getrange(byte[] key, long startInclusive, long endInclusive);

    Future<byte[]> getset(byte[] key, byte[] value);

    Future<Long> incr(byte[] key);

    Future<Long> incrby(byte[] key, long delta);

    Future<Double> incrbyfloat(byte[] key, double delta);

    Future<List<byte[]>> mget(byte[]... keys);

    Future<Void> mset(byte[]... keysvalues);

    Future<Boolean> msetnx(byte[]... keysvalues);
    
    Future<Boolean> set(byte[] key, byte[] value);

    Future<Boolean> set(byte[] key, byte[] value, SetParams params);

    Future<Boolean> setbit(byte[] key, long offset, boolean bit);

    Future<Long> setrange(byte[] key, long offset, byte[] value);

    Future<Long> strlen(byte[] key);
}
