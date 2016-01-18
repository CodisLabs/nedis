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

/**
 * You can not call these methods except {@code PING} if the
 * {@link io.codis.nedis.NedisClient} is borrowed from a
 * {@link io.codis.nedis.NedisClientPool}.
 * 
 * @author Apache9
 * @see http://redis.io/commands#connection
 */
public interface ConnectionCommands {

    Future<Void> auth(byte[] password);

    Future<byte[]> echo(byte[] msg);

    Future<String> ping();

    Future<Void> quit();

    Future<Void> select(int index);
}
