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

import java.nio.charset.StandardCharsets;

/**
 * @author Apache9
 */
public enum RedisCommand {
    APPEND, ASKING, AUTH, BGREWRITEAOF, BGSAVE, BITCOUNT, BITOP, BITPOS, BLPOP, BRPOP, BRPOPLPUSH,
    CLIENT, CLUSTER, CONFIG, DBSIZE, DEBUG, DECR, DECRBY, DEL, DISCARD, DUMP, ECHO, EVAL, EVALSHA,
    EXEC, EXISTS, EXPIRE, EXPIREAT, FLUSHALL, FLUSHDB, GET, GETBIT, GETRANGE, GETSET, HDEL,
    HEXISTS, HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN, HMGET, HMSET, HSCAN, HSET, HSETNX,
    HVALS, INCR, INCRBY, INCRBYFLOAT, INFO, KEYS, LASTSAVE, LINDEX, LINSERT, LLEN, LPOP, LPUSH,
    LPUSHX, LRANGE, LREM, LSET, LTRIM, MGET, MIGRATE, MONITOR, MOVE, MSET, MSETNX, MULTI, OBJECT,
    PERSIST, PEXPIRE, PEXPIREAT, PFADD, PFCOUNT, PFMERGE, PING, PSETEX, PSUBSCRIBE, PTTL, PUBLISH,
    PUBSUB, PUNSUBSCRIBE, QUIT, RANDOMKEY, RENAME, RENAMENX, RENAMEX, RESTORE, ROLE, RPOP,
    RPOPLPUSH, RPUSH, RPUSHX, SADD, SAVE, SCAN, SCARD, SCRIPT, SDIFF, SDIFFSTORE, SELECT, SENTINEL,
    SET, SETBIT, SETEX, SETNX, SETRANGE, SHUTDOWN, SINTER, SINTERSTORE, SISMEMBER, SLAVEOF,
    SLOWLOG, SMEMBERS, SMOVE, SORT, SPOP, SRANDMEMBER, SREM, SSCAN, STRLEN, SUBSCRIBE, SUBSTR,
    SUNION, SUNIONSTORE, SYNC, TIME, TTL, TYPE, UNSUBSCRIBE, UNWATCH, WAIT, WATCH, ZADD, ZCARD,
    ZCOUNT, ZINCRBY, ZINTERSTORE, ZLEXCOUNT, ZRANGE, ZRANGEBYLEX, ZRANGEBYSCORE, ZRANK, ZREM,
    ZREMRANGEBYLEX, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYLEX, ZREVRANGEBYSCORE,
    ZREVRANK, ZSCAN, ZSCORE, ZUNIONSTORE;

    public final byte[] raw;

    RedisCommand() {
        raw = name().getBytes(StandardCharsets.UTF_8);
    }

}
