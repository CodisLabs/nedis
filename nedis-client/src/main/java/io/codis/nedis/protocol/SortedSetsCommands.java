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
 * @see http://redis.io/commands#sorted_set
 */
public interface SortedSetsCommands {

    Future<Long> zadd(byte[] key, double score, byte[] member);

    Future<Long> zadd(byte[] key, Map<byte[], Double> member2Score);

    Future<Long> zcard(byte[] key);

    Future<Long> zcount(byte[] key, byte[] min, byte[] max);

    Future<Double> zincrby(byte[] key, double delta, byte[] member);

    Future<Long> zinterstore(byte[] dst, byte[]... keys);

    Future<Long> zinterstore(byte[] dst, ZSetOpParams params);

    Future<Long> zlexcount(byte[] key, byte[] min, byte[] max);

    Future<List<byte[]>> zrange(byte[] key, long startInclusive, long stopInclusive);

    Future<List<SortedSetEntry>> zrangeWithScores(byte[] key, long startInclusive,
            long stopInclusive);

    Future<List<byte[]>> zrangebylex(byte[] key, byte[] min, byte[] max);

    Future<List<byte[]>> zrangebylex(byte[] key, byte[] min, byte[] max, long offset, long count);

    Future<List<byte[]>> zrangebyscore(byte[] key, byte[] min, byte[] max);

    Future<List<byte[]>> zrangebyscore(byte[] key, byte[] min, byte[] max, long offset, long count);

    Future<List<SortedSetEntry>> zrangebyscoreWithScores(byte[] key, byte[] min, byte[] max);

    Future<List<SortedSetEntry>> zrangebyscoreWithScores(byte[] key, byte[] min, byte[] max,
            long offset, long count);

    Future<Long> zrank(byte[] key, byte[] member);

    Future<Long> zrem(byte[] key, byte[]... members);

    Future<Long> zremrangebylex(byte[] key, byte[] min, byte[] max);

    Future<Long> zremrangebyrank(byte[] key, long startInclusive, long stopInclusive);

    Future<Long> zremrangebyscore(byte[] key, byte[] min, byte[] max);

    Future<List<byte[]>> zrevrange(byte[] key, long startInclusive, long stopInclusive);

    Future<List<SortedSetEntry>> zrevrangeWithScores(byte[] key, long startInclusive,
            long stopInclusive);

    Future<List<byte[]>> zrevrangebylex(byte[] key, byte[] max, byte[] min);

    Future<List<byte[]>> zrevrangebylex(byte[] key, byte[] max, byte[] min, long offset, long count);

    Future<List<byte[]>> zrevrangebyscore(byte[] key, byte[] max, byte[] min);

    Future<List<byte[]>> zrevrangebyscore(byte[] key, byte[] max, byte[] min, long offset,
            long count);

    Future<List<SortedSetEntry>> zrevrangebyscoreWithScores(byte[] key, byte[] max, byte[] min);

    Future<List<SortedSetEntry>> zrevrangebyscoreWithScores(byte[] key, byte[] max, byte[] min,
            long offset, long count);

    Future<Long> zrevrank(byte[] key, byte[] member);

    Future<ScanResult<SortedSetEntry>> zscan(byte[] key, ScanParams params);

    Future<Double> zscore(byte[] key, byte[] member);

    Future<Long> zunionstore(byte[] dst, byte[]... keys);

    Future<Long> zunionstore(byte[] dst, ZSetOpParams params);
}
