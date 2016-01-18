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

import java.util.List;
import java.util.Map;

/**
 * @author Apache9
 * @see http://redis.io/commands#hash
 */
public interface HashesCommands {

    Future<Long> hdel(byte[] key, byte[]... fields);

    Future<Boolean> hexists(byte[] key, byte[] field);

    Future<byte[]> hget(byte[] key, byte[] field);

    Future<Map<byte[], byte[]>> hgetall(byte[] key);

    Future<Long> hincrby(byte[] key, byte[] field, long delta);

    Future<Double> hincrbyfloat(byte[] key, byte[] field, double delta);

    Future<List<byte[]>> hkeys(byte[] key);

    Future<Long> hlen(byte[] key);

    Future<List<byte[]>> hmget(byte[] key, byte[]... fields);

    Future<Void> hmset(byte[] key, Map<byte[], byte[]> field2Value);

    Future<ScanResult<HashEntry>> hscan(byte[] key, ScanParams params);

    Future<Boolean> hset(byte[] key, byte[] field, byte[] value);

    Future<Boolean> hsetnx(byte[] key, byte[] field, byte[] value);

    Future<List<byte[]>> hvals(byte[] key);
}
