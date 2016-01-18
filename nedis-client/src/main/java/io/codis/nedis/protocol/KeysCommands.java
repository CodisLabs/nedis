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

/**
 * {@code WAIT} is not supported yet.
 * 
 * @author Apache9
 * @see http://redis.io/commands#generic
 */
public interface KeysCommands {

    Future<Long> del(byte[]... keys);

    Future<byte[]> dump(byte[] key);

    Future<Boolean> exists(byte[] key);

    Future<Long> exists(byte[]... keys);

    Future<Boolean> expire(byte[] key, long seconds);

    Future<Boolean> expireat(byte[] key, long unixTimeSeconds);

    Future<List<byte[]>> keys(byte[] pattern);

    Future<Void> migrate(byte[] host, int port, byte[] key, int dstDb, long timeoutMs);

    Future<Boolean> move(byte[] key, int db);

    Future<Boolean> persist(byte[] key);

    Future<Boolean> pexpire(byte[] key, long millis);

    Future<Boolean> pexpireat(byte[] key, long unixTimeMs);

    Future<Long> pttl(byte[] key);

    Future<byte[]> randomkey();

    Future<Void> rename(byte[] key, byte[] newKey);

    Future<Boolean> renamenx(byte[] key, byte[] newKey);

    Future<Void> restore(byte[] key, int ttlMs, byte[] serializedValue, boolean replace);

    Future<ScanResult<byte[]>> scan(ScanParams params);

    Future<List<byte[]>> sort(byte[] key);

    Future<List<byte[]>> sort(byte[] key, SortParams params);

    Future<Long> sort(byte[] key, byte[] dst);

    Future<Long> sort(byte[] key, SortParams params, byte[] dst);

    Future<Long> ttl(byte[] key);

    Future<String> type(byte[] key);
}
