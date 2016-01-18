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
 * {@code MONITOR}, {@code SHUTDOWN} and {@code SLOWLOG} are not supported yet.
 * 
 * @author Apache9
 * @see http://redis.io/commands#server
 */
public interface ServerCommands {

    Future<Void> bgrewriteaof();

    Future<Void> bgsave();

    Future<byte[]> clientGetname();

    Future<Void> clientKill(byte[] addr);

    Future<byte[]> clientList();

    Future<Void> clientSetname(byte[] name);

    Future<List<byte[]>> configGet(byte[] pattern);

    Future<Void> configResetstat();

    Future<Void> configRewrite();

    Future<Void> configSet(byte[] name, byte[] value);

    Future<Long> dbsize();

    Future<Void> flushall();

    Future<Void> flushdb();

    Future<byte[]> info();

    Future<byte[]> info(byte[] section);

    Future<Long> lastsave();

    Future<List<byte[]>> role();

    Future<Void> save(boolean save);

    Future<Void> slaveof(String host, int port);

    Future<Void> sync();

    Future<List<byte[]>> time();
}
