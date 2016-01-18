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

import java.util.Set;

/**
 * @author zhangduo
 * @see http://redis.io/commands#set
 */
public interface SetsCommands {

    Future<Long> sadd(byte[] key, byte[]... members);

    Future<Long> scard(byte[] key);

    Future<Set<byte[]>> sdiff(byte[]... keys);

    Future<Long> sdiffstore(byte[] dst, byte[]... keys);

    Future<Set<byte[]>> sinter(byte[]... keys);

    Future<Long> sinterstore(byte[] dst, byte[]... keys);

    Future<Boolean> sismember(byte[] key, byte[] member);

    Future<Set<byte[]>> smembers(byte[] key);

    Future<Boolean> smove(byte[] src, byte[] dst, byte[] member);

    Future<byte[]> spop(byte[] key);

    Future<byte[]> srandmember(byte[] key);

    Future<Set<byte[]>> srandmember(byte[] key, long count);

    Future<Long> srem(byte[] key, byte[]... members);

    Future<ScanResult<byte[]>> sscan(byte[] key, ScanParams params);

    Future<Set<byte[]>> sunion(byte[]... keys);

    Future<Long> sunionstore(byte[] dst, byte[]... keys);

}
