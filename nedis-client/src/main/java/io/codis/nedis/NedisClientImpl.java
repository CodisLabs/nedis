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
package io.codis.nedis;

import io.codis.nedis.handler.RedisDuplexHandler;
import io.codis.nedis.handler.RedisRequest;
import io.codis.nedis.handler.TxnRedisRequest;
import io.codis.nedis.protocol.BitOp;
import io.codis.nedis.protocol.HashEntry;
import io.codis.nedis.protocol.RedisCommand;
import io.codis.nedis.protocol.RedisKeyword;
import io.codis.nedis.protocol.ScanParams;
import io.codis.nedis.protocol.ScanResult;
import io.codis.nedis.protocol.SetParams;
import io.codis.nedis.protocol.SortParams;
import io.codis.nedis.protocol.SortedSetEntry;
import io.codis.nedis.protocol.ZSetOpParams;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import static io.codis.nedis.handler.RedisRequestEncoder.encode;
import static io.codis.nedis.handler.RedisRequestEncoder.encodeReverse;
import static io.codis.nedis.protocol.RedisCommand.*;
import static io.codis.nedis.protocol.RedisKeyword.BY;
import static io.codis.nedis.protocol.RedisKeyword.COUNT;
import static io.codis.nedis.protocol.RedisKeyword.EX;
import static io.codis.nedis.protocol.RedisKeyword.FLUSH;
import static io.codis.nedis.protocol.RedisKeyword.GETNAME;
import static io.codis.nedis.protocol.RedisKeyword.KILL;
import static io.codis.nedis.protocol.RedisKeyword.LIMIT;
import static io.codis.nedis.protocol.RedisKeyword.LIST;
import static io.codis.nedis.protocol.RedisKeyword.LOAD;
import static io.codis.nedis.protocol.RedisKeyword.MATCH;
import static io.codis.nedis.protocol.RedisKeyword.NX;
import static io.codis.nedis.protocol.RedisKeyword.PX;
import static io.codis.nedis.protocol.RedisKeyword.REPLACE;
import static io.codis.nedis.protocol.RedisKeyword.RESETSTAT;
import static io.codis.nedis.protocol.RedisKeyword.REWRITE;
import static io.codis.nedis.protocol.RedisKeyword.SETNAME;
import static io.codis.nedis.protocol.RedisKeyword.STORE;
import static io.codis.nedis.protocol.RedisKeyword.WITHSCORES;
import static io.codis.nedis.protocol.RedisKeyword.XX;
import static io.codis.nedis.util.NedisUtils.toBytes;
import static io.codis.nedis.util.NedisUtils.toParamsReverse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.naming.OperationNotSupportedException;

/**
 * @author Apache9
 */
public class NedisClientImpl implements NedisClient {

    private final Channel channel;

    private final NedisClientPool pool;

    private final PromiseConverter<ScanResult<byte[]>> arrayScanResultConverter;

    private final PromiseConverter<Boolean> booleanConverter;

    private final PromiseConverter<List<Boolean>> booleanListConverter;

    private final PromiseConverter<byte[]> bytesConverter;

    private final PromiseConverter<Double> doubleConverter;

    private final PromiseConverter<ScanResult<HashEntry>> hashScanResultConverter;

    private final PromiseConverter<List<byte[]>> listConverter;

    private final PromiseConverter<Long> longConverter;

    private final PromiseConverter<Map<byte[], byte[]>> mapConverter;

    private final PromiseConverter<Object> objectConverter;

    private final PromiseConverter<List<Object>> objectListConverter;

    private final PromiseConverter<Set<byte[]>> setConverter;

    private final PromiseConverter<List<SortedSetEntry>> sortedSetEntryListConverter;

    private final PromiseConverter<ScanResult<SortedSetEntry>> sortedSetScanResultConverter;

    private final PromiseConverter<String> stringConverter;

    private final PromiseConverter<Void> voidConverter;

    public NedisClientImpl(Channel channel, NedisClientPool pool) {
        this.channel = channel;
        this.pool = pool;
        EventLoop eventLoop = channel.eventLoop();
        this.listConverter = PromiseConverter.toList(eventLoop);
        this.booleanConverter = PromiseConverter.toBoolean(eventLoop);
        this.bytesConverter = PromiseConverter.toBytes(eventLoop);
        this.doubleConverter = PromiseConverter.toDouble(eventLoop);
        this.longConverter = PromiseConverter.toLong(eventLoop);
        this.objectConverter = PromiseConverter.toObject(eventLoop);
        this.stringConverter = PromiseConverter.toString(eventLoop);
        this.voidConverter = PromiseConverter.toVoid(eventLoop);
        this.arrayScanResultConverter = PromiseConverter.toArrayScanResult(eventLoop);
        this.mapConverter = PromiseConverter.toMap(eventLoop);
        this.hashScanResultConverter = PromiseConverter.toHashScanResult(eventLoop);
        this.setConverter = PromiseConverter.toSet(eventLoop);
        this.sortedSetEntryListConverter = PromiseConverter.toSortedSetEntryList(eventLoop);
        this.sortedSetScanResultConverter = PromiseConverter.toSortedSetScanResult(eventLoop);
        this.booleanListConverter = PromiseConverter.toBooleanList(eventLoop);
        this.objectListConverter = PromiseConverter.toObjectList(eventLoop);
    }

    @Override
    public Future<Long> append(byte[] key, byte[] value) {
        return execCmd(longConverter, encode(channel.alloc(), APPEND.raw, key, value));
    }

    @Override
    public Future<Void> auth(byte[] password) {
        if (pool != null) {
            return eventLoop().newFailedFuture(
                    new OperationNotSupportedException(
                            "'auth' is not allowed on a pooled connection"));
        }
        return auth0(password);
    }

    Future<Void> auth0(byte[] password) {
        return execCmd(voidConverter, encode(channel.alloc(), AUTH.raw, password));
    }

    @Override
    public Future<Void> bgrewriteaof() {
        return execCmd(voidConverter, encode(channel.alloc(), BGREWRITEAOF.raw));
    }

    @Override
    public Future<Void> bgsave() {
        return execCmd(voidConverter, encode(channel.alloc(), BGSAVE.raw));
    }

    @Override
    public Future<Long> bitcount(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), BITCOUNT.raw, key));
    }

    @Override
    public Future<Long> bitcount(byte[] key, long startInclusive, long endInclusive) {
        return execCmd(
                longConverter,
                encode(channel.alloc(), BITCOUNT.raw, toBytes(startInclusive),
                        toBytes(endInclusive)));
    }

    @Override
    public Future<Long> bitop(BitOp op, byte[] dst, byte[]... keys) {
        return execCmd(longConverter,
                encode(channel.alloc(), BITOP.raw, toParamsReverse(keys, op.raw, dst)));
    }

    @Override
    public Future<Long> bitpos(byte[] key, boolean bit) {
        return execCmd(longConverter, encode(channel.alloc(), BITPOS.raw, key, toBytes(bit)));
    }

    @Override
    public Future<Long> bitpos(byte[] key, boolean bit, long startInclusive) {
        return execCmd(longConverter,
                encode(channel.alloc(), BITPOS.raw, key, toBytes(bit), toBytes(startInclusive)));
    }

    @Override
    public Future<Long> bitpos(byte[] key, boolean bit, long startInclusive, long endInclusive) {
        return execCmd(
                longConverter,
                encode(channel.alloc(), BITPOS.raw, key, toBytes(bit), toBytes(startInclusive),
                        toBytes(endInclusive)));
    }

    @Override
    public Future<List<byte[]>> blpop(long timeoutSeconds, byte[]... keys) {
        return execCmd(listConverter,
                encode(channel.alloc(), BLPOP.raw, keys, toBytes(timeoutSeconds)));
    }

    @Override
    public Future<List<byte[]>> brpop(long timeoutSeconds, byte[]... keys) {
        return execCmd(listConverter,
                encode(channel.alloc(), BRPOP.raw, keys, toBytes(timeoutSeconds)));
    }

    @Override
    public Future<byte[]> brpoplpush(byte[] src, byte[] dst, long timeoutSeconds) {
        return execCmd(bytesConverter,
                encode(channel.alloc(), BRPOPLPUSH.raw, src, dst, toBytes(timeoutSeconds)));
    }

    @Override
    public Future<byte[]> clientGetname() {
        return execCmd(bytesConverter, encode(channel.alloc(), CLIENT.raw, GETNAME.raw));
    }

    @Override
    public Future<Void> clientKill(byte[] addr) {
        return execCmd(voidConverter, encode(channel.alloc(), CLIENT.raw, KILL.raw));
    }

    @Override
    public Future<byte[]> clientList() {
        return execCmd(bytesConverter, encode(channel.alloc(), CLIENT.raw, LIST.raw));
    }

    @Override
    public Future<Void> clientSetname(byte[] name) {
        if (pool != null) {
            return eventLoop().newFailedFuture(
                    new OperationNotSupportedException(
                            "'client setname' is not allowed on a pooled connection"));
        }
        return clientSetname0(name);
    }

    Future<Void> clientSetname0(byte[] name) {
        return execCmd(voidConverter, encode(channel.alloc(), CLIENT.raw, SETNAME.raw, name));
    }

    @Override
    public ChannelFuture close() {
        return channel.close();
    }

    @Override
    public ChannelFuture closeFuture() {
        return channel.closeFuture();
    }

    @Override
    public Future<List<byte[]>> configGet(byte[] pattern) {
        return execCmd(listConverter, encode(channel.alloc(), CONFIG.raw, RedisKeyword.GET.raw));
    }

    @Override
    public Future<Void> configResetstat() {
        return execCmd(voidConverter, encode(channel.alloc(), CONFIG.raw, RESETSTAT.raw));
    }

    @Override
    public Future<Void> configRewrite() {
        return execCmd(voidConverter, encode(channel.alloc(), CONFIG.raw, REWRITE.raw));
    }

    @Override
    public Future<Void> configSet(byte[] name, byte[] value) {
        return execCmd(voidConverter, encode(channel.alloc(), CONFIG.raw, RedisKeyword.SET.raw));
    }

    @Override
    public Future<Long> dbsize() {
        return execCmd(longConverter, encode(channel.alloc(), DBSIZE.raw));
    }

    @Override
    public Future<Long> decr(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), DECR.raw, key));
    }

    @Override
    public Future<Long> decrby(byte[] key, long delta) {
        return execCmd(longConverter, encode(channel.alloc(), DECRBY.raw, key, toBytes(delta)));
    }

    @Override
    public Future<Long> del(byte[]... keys) {
        return execCmd(longConverter, encode(channel.alloc(), DEL.raw, keys));
    }

    @Override
    public Future<Void> discard() {
        return execTxnCmd(voidConverter, DISCARD);
    }

    @Override
    public Future<byte[]> dump(byte[] key) {
        return execCmd(bytesConverter, encode(channel.alloc(), DUMP.raw, key));
    }

    @Override
    public Future<byte[]> echo(byte[] msg) {
        return execCmd(bytesConverter, encode(channel.alloc(), ECHO.raw, msg));
    }

    private ByteBuf encodeSortParams(RedisCommand cmd, byte[] key, SortParams sort, byte[] dst) {
        List<byte[]> params = new ArrayList<>();
        params.add(key);
        if (sort.by() != null) {
            params.add(BY.raw);
            params.add(sort.by());
        }
        if (!sort.limit().isEmpty()) {
            params.add(LIMIT.raw);
            params.addAll(sort.limit());
        }
        if (!sort.get().isEmpty()) {
            params.addAll(sort.get());
        }
        if (sort.order() != null) {
            params.add(sort.order());
        }
        if (sort.getAlpha() != null) {
            params.add(sort.getAlpha());
        }
        if (dst != null) {
            params.add(STORE.raw);
            params.add(dst);
        }
        return encode(channel.alloc(), cmd.raw, params);
    }

    private ByteBuf encodeZSetOpParams(RedisCommand cmd, byte[] dst, ZSetOpParams params) {
        byte[][] p = new byte[2 + params.keys().size() + params.weights().size()
                + (params.aggregate() != null ? 1 : 0)][];
        p[0] = dst;
        p[1] = toBytes(params.keys().size());
        int i = 2;
        for (byte[] key: params.keys()) {
            p[i++] = key;
        }
        for (byte[] weight: params.weights()) {
            p[i++] = weight;
        }
        if (params.aggregate() != null) {
            p[i] = params.aggregate().raw;
        }
        return encode(channel.alloc(), cmd.raw, p);
    }

    @Override
    public Future<Object> eval(byte[] script, int numKeys, byte[]... keysvalues) {
        return execCmd(objectConverter,
                encodeReverse(channel.alloc(), EVAL.raw, keysvalues, script, toBytes(numKeys)));
    }

    @Override
    public Future<Object> evalsha(byte[] sha1, int numKeys, byte[]... keysvalues) {
        return execCmd(objectConverter,
                encodeReverse(channel.alloc(), EVALSHA.raw, keysvalues, sha1, toBytes(numKeys)));
    }

    @Override
    public EventLoop eventLoop() {
        return channel.eventLoop();
    }

    @Override
    public Future<List<Object>> exec() {
        return execTxnCmd(objectListConverter, EXEC);
    }

    @Override
    public Future<Object> execCmd(byte[] cmd, byte[]... params) {
        return execCmd(objectConverter, encode(channel.alloc(), cmd, params));
    }

    private <T> Future<T> execCmd(PromiseConverter<T> converter, ByteBuf buf) {
        Promise<T> promise = converter.newPromise();
        execCmd0(buf).addListener(converter.newListener(promise));
        return promise;
    }

    private Future<Object> execCmd0(ByteBuf buf) {
        Promise<Object> promise = eventLoop().newPromise();
        RedisRequest req = new RedisRequest(promise, buf);
        channel.writeAndFlush(req);
        return promise;
    }

    private <T> Future<ScanResult<T>> execScanCmd(PromiseConverter<ScanResult<T>> converter,
            RedisCommand cmd, byte[] key, ScanParams params) {
        List<byte[]> p = new ArrayList<>();
        if (key != null) {
            p.add(key);
        }
        p.add(params.cursor());
        if (params.match() != null) {
            p.add(MATCH.raw);
            p.add(params.match());
        }
        if (params.count() > 0) {
            p.add(COUNT.raw);
            p.add(toBytes(params.count()));
        }
        return execCmd(converter, encode(channel.alloc(), cmd.raw, p));
    }

    private <T> Future<T> execTxnCmd(PromiseConverter<T> converter, RedisCommand cmd) {
        Promise<Object> rawPromise = eventLoop().newPromise();
        channel.writeAndFlush(new TxnRedisRequest(rawPromise, cmd));
        Promise<T> promise = converter.newPromise();
        rawPromise.addListener(converter.newListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> exists(byte[] key) {
        return execCmd(booleanConverter, encode(channel.alloc(), EXISTS.raw, key));
    }

    @Override
    public Future<Long> exists(byte[]... keys) {
        return execCmd(longConverter, encode(channel.alloc(), EXISTS.raw, keys));
    }

    @Override
    public Future<Boolean> expire(byte[] key, long seconds) {
        return execCmd(booleanConverter, encode(channel.alloc(), EXPIRE.raw, key, toBytes(seconds)));
    }

    @Override
    public Future<Boolean> expireat(byte[] key, long unixTimeSeconds) {
        return execCmd(booleanConverter,
                encode(channel.alloc(), EXPIREAT.raw, key, toBytes(unixTimeSeconds)));
    }

    @Override
    public Future<Void> flushall() {
        return execCmd(voidConverter, encode(channel.alloc(), FLUSHALL.raw));
    }

    @Override
    public Future<Void> flushdb() {
        return execCmd(voidConverter, encode(channel.alloc(), FLUSHDB.raw));
    }

    @Override
    public Future<byte[]> get(byte[] key) {
        return execCmd(bytesConverter, encode(channel.alloc(), GET.raw, key));
    }

    @Override
    public Future<Boolean> getbit(byte[] key, long offset) {
        return execCmd(booleanConverter, encode(channel.alloc(), GETBIT.raw, key, toBytes(offset)));
    }

    @Override
    public Future<byte[]> getrange(byte[] key, long startInclusive, long endInclusive) {
        return execCmd(
                bytesConverter,
                encode(channel.alloc(), GETRANGE.raw, key, toBytes(startInclusive),
                        toBytes(endInclusive)));
    }

    @Override
    public Future<byte[]> getset(byte[] key, byte[] value) {
        return execCmd(bytesConverter, encode(channel.alloc(), GETSET.raw, key, value));
    }

    @Override
    public Future<Long> hdel(byte[] key, byte[]... fields) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), HDEL.raw, fields, key));
    }

    @Override
    public Future<Boolean> hexists(byte[] key, byte[] field) {
        return execCmd(booleanConverter, encode(channel.alloc(), HEXISTS.raw, key, field));
    }

    @Override
    public Future<byte[]> hget(byte[] key, byte[] field) {
        return execCmd(bytesConverter, encode(channel.alloc(), HGET.raw, key, field));
    }

    @Override
    public Future<Map<byte[], byte[]>> hgetall(byte[] key) {
        return execCmd(mapConverter, encode(channel.alloc(), HGETALL.raw, key));
    }

    @Override
    public Future<Long> hincrby(byte[] key, byte[] field, long delta) {
        return execCmd(longConverter,
                encode(channel.alloc(), HINCRBY.raw, key, field, toBytes(delta)));
    }

    @Override
    public Future<Double> hincrbyfloat(byte[] key, byte[] field, double delta) {
        return execCmd(doubleConverter,
                encode(channel.alloc(), HINCRBYFLOAT.raw, key, field, toBytes(delta)));
    }

    @Override
    public Future<List<byte[]>> hkeys(byte[] key) {
        return execCmd(listConverter, encode(channel.alloc(), HKEYS.raw, key));
    }

    @Override
    public Future<Long> hlen(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), HLEN.raw, key));
    }

    @Override
    public Future<List<byte[]>> hmget(byte[] key, byte[]... fields) {
        return execCmd(listConverter, encodeReverse(channel.alloc(), HMGET.raw, fields, key));
    }

    @Override
    public Future<Void> hmset(byte[] key, Map<byte[], byte[]> field2Value) {
        byte[][] params = new byte[2 * field2Value.size() + 1][];
        params[0] = key;
        int i = 1;
        for (Map.Entry<byte[], byte[]> e: field2Value.entrySet()) {
            params[i++] = e.getKey();
            params[i++] = e.getValue();
        }
        return execCmd(voidConverter, encode(channel.alloc(), HMSET.raw, params));
    }

    @Override
    public Future<ScanResult<HashEntry>> hscan(byte[] key, ScanParams params) {
        return execScanCmd(hashScanResultConverter, HSCAN, key, params);
    }

    @Override
    public Future<Boolean> hset(byte[] key, byte[] field, byte[] value) {
        return execCmd(booleanConverter, encode(channel.alloc(), HSET.raw, key, field, value));
    }

    @Override
    public Future<Boolean> hsetnx(byte[] key, byte[] field, byte[] value) {
        return execCmd(booleanConverter, encode(channel.alloc(), HSETNX.raw, key, field, value));
    }

    @Override
    public Future<List<byte[]>> hvals(byte[] key) {
        return execCmd(listConverter, encode(channel.alloc(), HVALS.raw, key));
    }

    @Override
    public Future<Long> incr(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), INCR.raw, key));
    }

    @Override
    public Future<Long> incrby(byte[] key, long delta) {
        return execCmd(longConverter, encode(channel.alloc(), INCRBY.raw, key, toBytes(delta)));
    }

    @Override
    public Future<Double> incrbyfloat(byte[] key, double delta) {
        return execCmd(doubleConverter,
                encode(channel.alloc(), INCRBYFLOAT.raw, key, toBytes(delta)));
    }

    @Override
    public Future<byte[]> info() {
        return execCmd(bytesConverter, encode(channel.alloc(), INFO.raw));
    }

    @Override
    public Future<byte[]> info(byte[] section) {
        return execCmd(bytesConverter, encode(channel.alloc(), INFO.raw, section));
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public Future<List<byte[]>> keys(byte[] pattern) {
        return execCmd(listConverter, encode(channel.alloc(), KEYS.raw, pattern));
    }

    @Override
    public Future<Long> lastsave() {
        return execCmd(longConverter, encode(channel.alloc(), LASTSAVE.raw));
    }

    @Override
    public Future<byte[]> lindex(byte[] key, long index) {
        return execCmd(bytesConverter, encode(channel.alloc(), LINDEX.raw, key, toBytes(index)));
    }

    @Override
    public Future<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return execCmd(longConverter,
                encode(channel.alloc(), LINSERT.raw, key, where.raw, pivot, value));
    }

    @Override
    public Future<Long> llen(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), LLEN.raw, key));
    }

    @Override
    public Future<byte[]> lpop(byte[] key) {
        return execCmd(bytesConverter, encode(channel.alloc(), LPOP.raw, key));
    }

    @Override
    public Future<Long> lpush(byte[] key, byte[]... values) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), LPUSH.raw, values, key));
    }

    @Override
    public Future<Long> lpushx(byte[] key, byte[] value) {
        return execCmd(longConverter, encode(channel.alloc(), LPUSHX.raw, key, value));
    }

    @Override
    public Future<List<byte[]>> lrange(byte[] key, long startInclusive, long stopInclusive) {
        return execCmd(
                listConverter,
                encode(channel.alloc(), LRANGE.raw, key, toBytes(startInclusive),
                        toBytes(stopInclusive)));
    }

    @Override
    public Future<Long> lrem(byte[] key, long count, byte[] value) {
        return execCmd(longConverter, encode(channel.alloc(), LREM.raw, key, toBytes(count), value));
    }

    @Override
    public Future<byte[]> lset(byte[] key, long index, byte[] value) {
        return execCmd(bytesConverter,
                encode(channel.alloc(), LSET.raw, key, toBytes(index), value));
    }

    @Override
    public Future<Void> ltrim(byte[] key, long startInclusive, long stopInclusive) {
        return execCmd(
                voidConverter,
                encode(channel.alloc(), LTRIM.raw, key, toBytes(startInclusive),
                        toBytes(stopInclusive)));
    }

    @Override
    public Future<List<byte[]>> mget(byte[]... keys) {
        return execCmd(listConverter, encode(channel.alloc(), MGET.raw, keys));
    }

    @Override
    public Future<Void> migrate(byte[] host, int port, byte[] key, int dstDb, long timeoutMs) {
        return execCmd(
                voidConverter,
                encode(channel.alloc(), MIGRATE.raw, host, toBytes(port), key, toBytes(dstDb),
                        toBytes(timeoutMs)));
    }

    @Override
    public Future<Boolean> move(byte[] key, int db) {
        return execCmd(booleanConverter, encode(channel.alloc(), MOVE.raw, key, toBytes(db)));
    }

    @Override
    public Future<Void> mset(byte[]... keysvalues) {
        return execCmd(voidConverter, encode(channel.alloc(), MSET.raw, keysvalues));
    }

    @Override
    public Future<Boolean> msetnx(byte[]... keysvalues) {
        return execCmd(booleanConverter, encode(channel.alloc(), MSETNX.raw, keysvalues));
    }

    @Override
    public Future<Void> multi() {
        return execTxnCmd(voidConverter, MULTI);
    }

    @Override
    public Future<Boolean> persist(byte[] key) {
        return execCmd(booleanConverter, encode(channel.alloc(), PERSIST.raw, key));
    }

    @Override
    public Future<Boolean> pexpire(byte[] key, long millis) {
        return execCmd(booleanConverter, encode(channel.alloc(), PEXPIRE.raw, toBytes(millis)));
    }

    @Override
    public Future<Boolean> pexpireat(byte[] key, long unixTimeMs) {
        return execCmd(booleanConverter,
                encode(channel.alloc(), PEXPIREAT.raw, toBytes(unixTimeMs)));
    }

    @Override
    public Future<Boolean> pfadd(byte[] key, byte[]... elements) {
        return execCmd(booleanConverter, encodeReverse(channel.alloc(), PFADD.raw, elements, key));
    }

    @Override
    public Future<Long> pfcount(byte[]... keys) {
        return execCmd(longConverter, encode(channel.alloc(), PFCOUNT.raw, keys));
    }

    @Override
    public Future<Void> pfmerge(byte[] dst, byte[]... keys) {
        return execCmd(voidConverter, encodeReverse(channel.alloc(), PFMERGE.raw, keys, dst));
    }

    @Override
    public Future<String> ping() {
        return execCmd(stringConverter, encode(channel.alloc(), PING.raw));
    }

    @Override
    public Future<Long> pttl(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), PTTL.raw, key));
    }

    @Override
    public Future<Void> quit() {
        if (pool != null) {
            return eventLoop().newFailedFuture(
                    new OperationNotSupportedException(
                            "'quit' is not allowed on a pooled connection"));
        }
        return quit0();
    }

    Future<Void> quit0() {
        return execCmd(voidConverter, encode(channel.alloc(), QUIT.raw));
    }

    @Override
    public Future<byte[]> randomkey() {
        return execCmd(bytesConverter, encode(channel.alloc(), RANDOMKEY.raw));
    }

    @Override
    public void release() {
        if (pool != null && pool.exclusive()) {
            pool.release(this);
        }
    }

    @Override
    public Future<Void> rename(byte[] key, byte[] newKey) {
        return execCmd(voidConverter, encode(channel.alloc(), RENAME.raw, key, newKey));
    }

    @Override
    public Future<Boolean> renamenx(byte[] key, byte[] newKey) {
        return execCmd(booleanConverter, encode(channel.alloc(), RENAMENX.raw, key, newKey));
    }

    @Override
    public Future<Void> restore(byte[] key, int ttlMs, byte[] serializedValue, boolean replace) {
        if (replace) {
            return execCmd(
                    voidConverter,
                    encode(channel.alloc(), RESTORE.raw, key, toBytes(ttlMs), serializedValue,
                            REPLACE.raw));
        } else {
            return execCmd(voidConverter,
                    encode(channel.alloc(), RESTORE.raw, key, toBytes(ttlMs), serializedValue));
        }
    }

    @Override
    public Future<List<byte[]>> role() {
        return execCmd(listConverter, encode(channel.alloc(), ROLE.raw));
    }

    @Override
    public Future<byte[]> rpop(byte[] key) {
        return execCmd(bytesConverter, encode(channel.alloc(), RPOP.raw, key));
    }

    @Override
    public Future<byte[]> rpoplpush(byte[] src, byte[] dst) {
        return execCmd(bytesConverter, encode(channel.alloc(), RPOPLPUSH.raw, src, dst));
    }

    @Override
    public Future<Long> rpush(byte[] key, byte[]... values) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), RPUSH.raw, values, key));
    }

    @Override
    public Future<Long> rpushx(byte[] key, byte[] value) {
        return execCmd(longConverter, encode(channel.alloc(), RPUSHX.raw, key, value));
    }

    @Override
    public Future<Long> sadd(byte[] key, byte[]... members) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), SADD.raw, members, key));
    }

    @Override
    public Future<Void> save(boolean save) {
        return execCmd(voidConverter, encode(channel.alloc(), SAVE.raw));
    }

    @Override
    public Future<ScanResult<byte[]>> scan(ScanParams params) {
        return execScanCmd(arrayScanResultConverter, SCAN, null, params);
    }

    @Override
    public Future<Long> scard(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), SCARD.raw, key));
    }

    @Override
    public Future<List<Boolean>> scriptExists(byte[]... scripts) {
        return execCmd(booleanListConverter,
                encodeReverse(channel.alloc(), SCRIPT.raw, scripts, RedisKeyword.EXISTS.raw));
    }

    @Override
    public Future<Void> scriptFlush() {
        return execCmd(voidConverter, encode(channel.alloc(), SCRIPT.raw, FLUSH.raw));
    }

    @Override
    public Future<Void> scriptKill() {
        return execCmd(voidConverter, encode(channel.alloc(), SCRIPT.raw, KILL.raw));
    }

    @Override
    public Future<byte[]> scriptLoad(byte[] script) {
        return execCmd(bytesConverter, encode(channel.alloc(), SCRIPT.raw, LOAD.raw, script));
    }

    @Override
    public Future<Set<byte[]>> sdiff(byte[]... keys) {
        return execCmd(setConverter, encode(channel.alloc(), SDIFF.raw, keys));
    }

    @Override
    public Future<Long> sdiffstore(byte[] dst, byte[]... keys) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), SDIFFSTORE.raw, keys, dst));
    }

    @Override
    public Future<Void> select(int index) {
        if (pool != null) {
            return eventLoop().newFailedFuture(
                    new OperationNotSupportedException(
                            "'select' is not allowed on a pooled connection"));
        }
        return select0(index);
    }

    Future<Void> select0(int index) {
        return execCmd(voidConverter, encode(channel.alloc(), SELECT.raw, toBytes(index)));
    }

    @Override
    public Future<Boolean> set(byte[] key, byte[] value) {
        return execCmd(booleanConverter, encode(channel.alloc(), SET.raw, key, value));
    }

    @Override
    public Future<Boolean> set(byte[] key, byte[] value, SetParams params) {
        List<byte[]> p = new ArrayList<>();
        p.add(key);
        p.add(value);
        if (params.ex() > 0) {
            p.add(EX.raw);
            p.add(toBytes(params.ex()));
        } else if (params.px() > 0) {
            p.add(PX.raw);
            p.add(toBytes(params.px()));
        }
        if (params.nx()) {
            p.add(NX.raw);
        } else if (params.xx()) {
            p.add(XX.raw);
        }
        return execCmd(booleanConverter, encode(channel.alloc(), SET.raw, p));
    }

    @Override
    public Future<Boolean> setbit(byte[] key, long offset, boolean bit) {
        return execCmd(booleanConverter,
                encode(channel.alloc(), SETBIT.raw, key, toBytes(offset), toBytes(bit)));
    }

    @Override
    public Future<Long> setrange(byte[] key, long offset, byte[] value) {
        return execCmd(longConverter,
                encode(channel.alloc(), SETRANGE.raw, key, toBytes(offset), value));
    }

    @Override
    public Future<Long> setTimeout(final long timeoutMs) {
        return eventLoop().submit(new Callable<Long>() {

            @Override
            public Long call() {
                RedisDuplexHandler handler = channel.pipeline().get(RedisDuplexHandler.class);
                if (handler == null) {
                    return null;
                }
                long previousTimeoutMs = TimeUnit.NANOSECONDS.toMillis(handler.getTimeoutNs());
                handler.setTimeoutNs(TimeUnit.MILLISECONDS.toNanos(timeoutMs));
                return previousTimeoutMs;
            }

        });
    }

    @Override
    public Future<Set<byte[]>> sinter(byte[]... keys) {
        return execCmd(setConverter, encode(channel.alloc(), SINTER.raw, keys));
    }

    @Override
    public Future<Long> sinterstore(byte[] dst, byte[]... keys) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), SINTERSTORE.raw, keys, dst));
    }

    @Override
    public Future<Boolean> sismember(byte[] key, byte[] member) {
        return execCmd(booleanConverter, encode(channel.alloc(), SISMEMBER.raw, key, member));
    }

    @Override
    public Future<Void> slaveof(String host, int port) {
        return execCmd(voidConverter,
                encode(channel.alloc(), SLAVEOF.raw, toBytes(host), toBytes(port)));
    }

    @Override
    public Future<Set<byte[]>> smembers(byte[] key) {
        return execCmd(setConverter, encode(channel.alloc(), SMEMBERS.raw, key));
    }

    @Override
    public Future<Boolean> smove(byte[] src, byte[] dst, byte[] member) {
        return execCmd(booleanConverter, encode(channel.alloc(), SMOVE.raw, src, dst, member));
    }

    @Override
    public Future<List<byte[]>> sort(byte[] key) {
        return execCmd(listConverter, encode(channel.alloc(), SORT.raw, key));
    }

    @Override
    public Future<Long> sort(byte[] key, byte[] dst) {
        return execCmd(longConverter, encode(channel.alloc(), SORT.raw, key, STORE.raw, dst));
    }

    @Override
    public Future<List<byte[]>> sort(byte[] key, SortParams params) {
        return execCmd(listConverter, encodeSortParams(SORT, key, params, null));
    }

    @Override
    public Future<Long> sort(byte[] key, SortParams params, byte[] dst) {
        return execCmd(longConverter, encodeSortParams(SORT, key, params, dst));
    }

    @Override
    public Future<byte[]> spop(byte[] key) {
        return execCmd(bytesConverter, encode(channel.alloc(), SPOP.raw, key));
    }

    @Override
    public Future<byte[]> srandmember(byte[] key) {
        return execCmd(bytesConverter, encode(channel.alloc(), SRANDMEMBER.raw, key));
    }

    @Override
    public Future<Set<byte[]>> srandmember(byte[] key, long count) {
        return execCmd(setConverter, encode(channel.alloc(), SRANDMEMBER.raw, key, toBytes(count)));
    }

    @Override
    public Future<Long> srem(byte[] key, byte[]... members) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), SREM.raw, members, key));
    }

    @Override
    public Future<ScanResult<byte[]>> sscan(byte[] key, ScanParams params) {
        return execScanCmd(arrayScanResultConverter, SSCAN, key, params);
    }

    @Override
    public Future<Long> strlen(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), STRLEN.raw, key));
    }

    @Override
    public Future<Set<byte[]>> sunion(byte[]... keys) {
        return execCmd(setConverter, encode(channel.alloc(), SUNION.raw, keys));
    }

    @Override
    public Future<Long> sunionstore(byte[] dst, byte[]... keys) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), SUNIONSTORE.raw, keys, dst));
    }

    @Override
    public Future<Void> sync() {
        return execCmd(voidConverter, encode(channel.alloc(), SYNC.raw));
    }

    @Override
    public Future<List<byte[]>> time() {
        return execCmd(listConverter, encode(channel.alloc(), TIME.raw));
    }

    @Override
    public Future<Long> ttl(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), TTL.raw, key));
    }

    @Override
    public Future<String> type(byte[] key) {
        return execCmd(stringConverter, encode(channel.alloc(), TYPE.raw, key));
    }

    @Override
    public Future<Void> unwatch() {
        return execCmd(voidConverter, encode(channel.alloc(), UNWATCH.raw));
    }

    @Override
    public Future<Void> watch(byte[]... keys) {
        return execCmd(voidConverter, encode(channel.alloc(), WATCH.raw, keys));
    }

    @Override
    public Future<Long> zadd(byte[] key, double score, byte[] member) {
        return execCmd(longConverter,
                encode(channel.alloc(), ZADD.raw, key, toBytes(score), member));
    }

    @Override
    public Future<Long> zadd(byte[] key, Map<byte[], Double> member2Score) {
        byte[][] params = new byte[member2Score.size() * 2 + 1][];
        params[0] = key;
        int i = 1;
        for (Map.Entry<byte[], Double> e: member2Score.entrySet()) {
            params[i++] = toBytes(e.getValue().doubleValue());
            params[i++] = e.getKey();
        }
        return execCmd(longConverter, encode(channel.alloc(), ZADD.raw, params));
    }

    @Override
    public Future<Long> zcard(byte[] key) {
        return execCmd(longConverter, encode(channel.alloc(), ZCARD.raw, key));
    }

    @Override
    public Future<Long> zcount(byte[] key, byte[] min, byte[] max) {
        return execCmd(longConverter, encode(channel.alloc(), ZCOUNT.raw, key, min, max));
    }

    @Override
    public Future<Double> zincrby(byte[] key, double delta, byte[] member) {
        return execCmd(doubleConverter,
                encode(channel.alloc(), ZINCRBY.raw, key, toBytes(delta), member));
    }

    @Override
    public Future<Long> zinterstore(byte[] dst, byte[]... keys) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), ZINTERSTORE.raw, keys, dst));
    }

    @Override
    public Future<Long> zinterstore(byte[] dst, ZSetOpParams params) {
        return execCmd(longConverter, encodeZSetOpParams(ZINTERSTORE, dst, params));
    }

    @Override
    public Future<Long> zlexcount(byte[] key, byte[] min, byte[] max) {
        return execCmd(longConverter, encode(channel.alloc(), ZLEXCOUNT.raw, key, min, max));
    }

    @Override
    public Future<List<byte[]>> zrange(byte[] key, long startInclusive, long stopInclusive) {
        return execCmd(
                listConverter,
                encode(channel.alloc(), ZRANGE.raw, key, toBytes(startInclusive),
                        toBytes(stopInclusive)));
    }

    @Override
    public Future<List<byte[]>> zrangebylex(byte[] key, byte[] min, byte[] max) {
        return execCmd(listConverter, encode(channel.alloc(), ZRANGEBYLEX.raw, key, min, max));
    }

    @Override
    public Future<List<byte[]>> zrangebylex(byte[] key, byte[] min, byte[] max, long offset,
            long count) {
        return execCmd(
                listConverter,
                encode(channel.alloc(), ZRANGEBYLEX.raw, key, min, max, LIMIT.raw, toBytes(offset),
                        toBytes(count)));
    }

    @Override
    public Future<List<byte[]>> zrangebyscore(byte[] key, byte[] min, byte[] max) {
        return execCmd(listConverter, encode(channel.alloc(), ZRANGEBYSCORE.raw, key, min, max));
    }

    @Override
    public Future<List<byte[]>> zrangebyscore(byte[] key, byte[] min, byte[] max, long offset,
            long count) {
        return execCmd(
                listConverter,
                encode(channel.alloc(), ZRANGEBYSCORE.raw, key, min, max, LIMIT.raw,
                        toBytes(offset), toBytes(count)));
    }

    @Override
    public Future<List<SortedSetEntry>> zrangebyscoreWithScores(byte[] key, byte[] min, byte[] max) {
        return execCmd(sortedSetEntryListConverter,
                encode(channel.alloc(), ZRANGEBYSCORE.raw, key, min, max, WITHSCORES.raw));
    }

    @Override
    public Future<List<SortedSetEntry>> zrangebyscoreWithScores(byte[] key, byte[] min, byte[] max,
            long offset, long count) {
        return execCmd(
                sortedSetEntryListConverter,
                encode(channel.alloc(), ZRANGEBYSCORE.raw, key, min, max, WITHSCORES.raw,
                        LIMIT.raw, toBytes(offset), toBytes(count)));
    }

    @Override
    public Future<List<SortedSetEntry>> zrangeWithScores(byte[] key, long startInclusive,
            long stopInclusive) {
        return execCmd(
                sortedSetEntryListConverter,
                encode(channel.alloc(), ZRANGE.raw, key, toBytes(startInclusive),
                        toBytes(stopInclusive), WITHSCORES.raw));
    }

    @Override
    public Future<Long> zrank(byte[] key, byte[] member) {
        return execCmd(longConverter, encode(channel.alloc(), ZRANK.raw, key, member));
    }

    @Override
    public Future<Long> zrem(byte[] key, byte[]... members) {
        return execCmd(longConverter,
                encode(channel.alloc(), ZREM.raw, toParamsReverse(members, key)));
    }

    @Override
    public Future<Long> zremrangebylex(byte[] key, byte[] min, byte[] max) {
        return execCmd(longConverter, encode(channel.alloc(), ZREMRANGEBYLEX.raw, key, min, max));
    }

    @Override
    public Future<Long> zremrangebyrank(byte[] key, long startInclusive, long stopInclusive) {
        return execCmd(
                longConverter,
                encode(channel.alloc(), ZREMRANGEBYRANK.raw, key, toBytes(startInclusive),
                        toBytes(stopInclusive)));
    }

    @Override
    public Future<Long> zremrangebyscore(byte[] key, byte[] min, byte[] max) {
        return execCmd(longConverter, encode(channel.alloc(), ZREMRANGEBYSCORE.raw, key, min, max));
    }

    @Override
    public Future<List<byte[]>> zrevrange(byte[] key, long startInclusive, long stopInclusive) {
        return execCmd(
                listConverter,
                encode(channel.alloc(), ZREVRANGE.raw, key, toBytes(startInclusive),
                        toBytes(stopInclusive)));
    }

    @Override
    public Future<List<byte[]>> zrevrangebylex(byte[] key, byte[] max, byte[] min) {
        return execCmd(listConverter, encode(channel.alloc(), ZREVRANGEBYLEX.raw, key, max, min));
    }

    @Override
    public Future<List<byte[]>> zrevrangebylex(byte[] key, byte[] max, byte[] min, long offset,
            long count) {
        return execCmd(
                listConverter,
                encode(channel.alloc(), ZREVRANGEBYLEX.raw, key, max, min, LIMIT.raw,
                        toBytes(offset), toBytes(count)));
    }

    @Override
    public Future<List<byte[]>> zrevrangebyscore(byte[] key, byte[] max, byte[] min) {
        return execCmd(listConverter, encode(channel.alloc(), ZREVRANGEBYSCORE.raw, key, max, min));
    }

    @Override
    public Future<List<byte[]>> zrevrangebyscore(byte[] key, byte[] max, byte[] min, long offset,
            long count) {
        return execCmd(
                listConverter,
                encode(channel.alloc(), ZREVRANGEBYSCORE.raw, key, max, min, LIMIT.raw,
                        toBytes(offset), toBytes(count)));
    }

    @Override
    public Future<List<SortedSetEntry>> zrevrangebyscoreWithScores(byte[] key, byte[] max,
            byte[] min) {
        return execCmd(sortedSetEntryListConverter,
                encode(channel.alloc(), ZREVRANGEBYSCORE.raw, key, max, min, WITHSCORES.raw));
    }

    @Override
    public Future<List<SortedSetEntry>> zrevrangebyscoreWithScores(byte[] key, byte[] max,
            byte[] min, long offset, long count) {
        return execCmd(
                sortedSetEntryListConverter,
                encode(channel.alloc(), ZREVRANGEBYSCORE.raw, key, max, min, WITHSCORES.raw,
                        LIMIT.raw, toBytes(offset), toBytes(count), WITHSCORES.raw));
    }

    @Override
    public Future<List<SortedSetEntry>> zrevrangeWithScores(byte[] key, long startInclusive,
            long stopInclusive) {
        return execCmd(
                sortedSetEntryListConverter,
                encode(channel.alloc(), ZREVRANGE.raw, key, toBytes(startInclusive),
                        toBytes(stopInclusive), WITHSCORES.raw));
    }

    @Override
    public Future<Long> zrevrank(byte[] key, byte[] member) {
        return execCmd(longConverter, encode(channel.alloc(), ZREVRANK.raw, key, member));
    }

    @Override
    public Future<ScanResult<SortedSetEntry>> zscan(byte[] key, ScanParams params) {
        return execScanCmd(sortedSetScanResultConverter, ZSCAN, key, params);
    }

    @Override
    public Future<Double> zscore(byte[] key, byte[] member) {
        return execCmd(doubleConverter, encode(channel.alloc(), ZSCORE.raw, key, member));
    }

    @Override
    public Future<Long> zunionstore(byte[] dst, byte[]... keys) {
        return execCmd(longConverter, encodeReverse(channel.alloc(), ZUNIONSTORE.raw, keys, dst));
    }

    @Override
    public Future<Long> zunionstore(byte[] dst, ZSetOpParams params) {
        return execCmd(longConverter, encodeZSetOpParams(ZUNIONSTORE, dst, params));
    }
}
