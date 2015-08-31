package com.github.apache9.nedis;

import static com.github.apache9.nedis.protocol.RedisCommand.*;
import static com.github.apache9.nedis.protocol.RedisKeyword.COUNT;
import static com.github.apache9.nedis.protocol.RedisKeyword.EX;
import static com.github.apache9.nedis.protocol.RedisKeyword.GETNAME;
import static com.github.apache9.nedis.protocol.RedisKeyword.KILL;
import static com.github.apache9.nedis.protocol.RedisKeyword.LIST;
import static com.github.apache9.nedis.protocol.RedisKeyword.MATCH;
import static com.github.apache9.nedis.protocol.RedisKeyword.NX;
import static com.github.apache9.nedis.protocol.RedisKeyword.PX;
import static com.github.apache9.nedis.protocol.RedisKeyword.REPLACE;
import static com.github.apache9.nedis.protocol.RedisKeyword.RESETSTAT;
import static com.github.apache9.nedis.protocol.RedisKeyword.REWRITE;
import static com.github.apache9.nedis.protocol.RedisKeyword.SETNAME;
import static com.github.apache9.nedis.protocol.RedisKeyword.XX;
import static com.github.apache9.nedis.util.NedisUtils.toBytes;
import static com.github.apache9.nedis.util.NedisUtils.toParams;
import static com.github.apache9.nedis.util.NedisUtils.toParamsReverse;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.naming.OperationNotSupportedException;

import com.github.apache9.nedis.handler.RedisDuplexHandler;
import com.github.apache9.nedis.protocol.BitOp;
import com.github.apache9.nedis.protocol.HashEntry;
import com.github.apache9.nedis.protocol.RedisCommand;
import com.github.apache9.nedis.protocol.RedisKeyword;
import com.github.apache9.nedis.protocol.ScanParams;
import com.github.apache9.nedis.protocol.ScanResult;
import com.github.apache9.nedis.protocol.SetParams;

/**
 * @author Apache9
 */
public class NedisClientImpl implements NedisClient {

    private final Channel channel;

    private final NedisClientPool pool;

    private final PromiseConverter<List<byte[]>> arrayConverter;

    private final PromiseConverter<ScanResult<byte[]>> arrayScanResultConverter;

    private final PromiseConverter<Boolean> booleanConverter;

    private final PromiseConverter<byte[]> bytesConverter;

    private final PromiseConverter<Double> doubleConverter;

    private final PromiseConverter<ScanResult<HashEntry>> hashScanResultConverter;

    private final PromiseConverter<Long> longConverter;

    private final PromiseConverter<Map<byte[], byte[]>> mapConverter;

    private final PromiseConverter<Object> objectConverter;

    private final PromiseConverter<String> stringConverter;

    private final PromiseConverter<Void> voidConverter;

    public NedisClientImpl(Channel channel, NedisClientPool pool) {
        this.channel = channel;
        this.pool = pool;
        EventLoop eventLoop = channel.eventLoop();
        this.arrayConverter = PromiseConverter.toArray(eventLoop);
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
    }

    private void addScanParams(List<byte[]> params, ScanParams scan) {
        params.add(scan.cursor());
        if (scan.match() != null) {
            params.add(MATCH.raw);
            params.add(scan.match());
        }
        if (scan.count() > 0) {
            params.add(COUNT.raw);
            params.add(toBytes(scan.count()));
        }
    }

    @Override
    public Future<Long> append(byte[] key, byte[] value) {
        return execCmd(longConverter, APPEND, key, value);
    }

    @Override
    public Future<Void> auth(byte[] password) {
        if (pool != null) {
            Promise<Void> promise = eventLoop().newPromise();
            promise.tryFailure(new OperationNotSupportedException(
                    "'auth' is not allowed on a pooled connection"));
            return promise;
        }
        return auth0(password);
    }

    Future<Void> auth0(byte[] password) {
        return execCmd(voidConverter, AUTH, password);
    }

    @Override
    public Future<Void> bgrewriteaof() {
        return execCmd(voidConverter, BGREWRITEAOF);
    }

    @Override
    public Future<Void> bgsave() {
        return execCmd(voidConverter, BGSAVE);
    }

    @Override
    public Future<Long> bitcount(byte[] key) {
        return execCmd(longConverter, BITCOUNT, key);
    }

    @Override
    public Future<Long> bitcount(byte[] key, long startInclusive, long endInclusive) {
        return execCmd(longConverter, BITCOUNT, toBytes(startInclusive), toBytes(endInclusive));
    }

    @Override
    public Future<Long> bitop(BitOp op, byte[] dst, byte[]... keys) {
        return execCmd(longConverter, BITOP, toParamsReverse(keys, op.raw, dst));
    }

    @Override
    public Future<Long> bitpos(byte[] key, boolean bit) {
        return execCmd(longConverter, BITPOS, key, toBytes(bit));
    }

    @Override
    public Future<Long> bitpos(byte[] key, boolean bit, long startInclusive) {
        return execCmd(longConverter, BITPOS, key, toBytes(bit), toBytes(startInclusive));
    }

    @Override
    public Future<Long> bitpos(byte[] key, boolean bit, long startInclusive, long endInclusive) {
        return execCmd(longConverter, BITPOS, key, toBytes(bit), toBytes(startInclusive),
                toBytes(endInclusive));
    }

    @Override
    public Future<List<byte[]>> blpop(long timeoutSeconds, byte[]... keys) {
        return execCmd(arrayConverter, BLPOP, toParams(keys, toBytes(timeoutSeconds)));
    }

    @Override
    public Future<List<byte[]>> brpop(long timeoutSeconds, byte[]... keys) {
        return execCmd(arrayConverter, BRPOP, toParams(keys, toBytes(timeoutSeconds)));
    }

    @Override
    public Future<byte[]> brpoplpush(byte[] src, byte[] dst, long timeoutSeconds) {
        return execCmd(bytesConverter, BRPOPLPUSH, src, dst, toBytes(timeoutSeconds));
    }

    @Override
    public Future<byte[]> clientGetname() {
        return execCmd(bytesConverter, CLIENT, GETNAME.raw);
    }

    @Override
    public Future<Void> clientKill(byte[] addr) {
        return execCmd(voidConverter, CLIENT, KILL.raw);
    }

    @Override
    public Future<byte[]> clientList() {
        return execCmd(bytesConverter, CLIENT, LIST.raw);
    }

    @Override
    public Future<Void> clientSetname(byte[] name) {
        if (pool != null) {
            Promise<Void> promise = eventLoop().newPromise();
            promise.tryFailure(new OperationNotSupportedException(
                    "'client setname' is not allowed on a pooled connection"));
            return promise;
        }
        return clientSetname0(name);
    }

    Future<Void> clientSetname0(byte[] name) {
        return execCmd(voidConverter, CLIENT, SETNAME.raw, name);
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
        return execCmd(arrayConverter, CONFIG, RedisKeyword.GET.raw);
    }

    @Override
    public Future<Void> configResetstat() {
        return execCmd(voidConverter, CONFIG, RESETSTAT.raw);
    }

    @Override
    public Future<Void> configRewrite() {
        return execCmd(voidConverter, CONFIG, REWRITE.raw);
    }

    @Override
    public Future<Void> configSet(byte[] name, byte[] value) {
        return execCmd(voidConverter, CONFIG, RedisKeyword.SET.raw);
    }

    @Override
    public Future<Long> dbsize() {
        return execCmd(longConverter, DBSIZE);
    }

    @Override
    public Future<Long> decr(byte[] key) {
        return execCmd(longConverter, DECR, key);
    }

    @Override
    public Future<Long> decrBy(byte[] key, long delta) {
        return execCmd(longConverter, DECRBY, key, toBytes(delta));
    }

    @Override
    public Future<Long> del(byte[]... keys) {
        return execCmd(longConverter, DEL, keys);
    }

    @Override
    public Future<byte[]> dump(byte[] key) {
        return execCmd(bytesConverter, DUMP, key);
    }

    @Override
    public Future<byte[]> echo(byte[] msg) {
        return execCmd(bytesConverter, ECHO, msg);
    }

    @Override
    public Future<Object> eval(byte[] script, int numKeys, byte[]... keysvalues) {
        return execCmd(objectConverter, EVAL, toParamsReverse(keysvalues, script, toBytes(numKeys)));
    }

    @Override
    public EventLoop eventLoop() {
        return channel.eventLoop();
    }

    @Override
    public Future<Object> execCmd(byte[] cmd, byte[]... params) {
        return execCmd(objectConverter, cmd, params);
    }

    private <T> Future<T> execCmd(PromiseConverter<T> converter, byte[] cmd, byte[]... params) {
        Promise<T> promise = converter.newPromise();
        execCmd0(cmd, params).addListener(converter.newListener(promise));
        return promise;
    }

    private <T> Future<T> execCmd(PromiseConverter<T> converter, RedisCommand cmd, byte[]... params) {
        return execCmd(converter, cmd.raw, params);
    }

    private Future<Object> execCmd0(byte[] cmd, byte[]... params) {
        Promise<Object> promise = eventLoop().newPromise();
        RedisRequest req = new RedisRequest(promise, toParamsReverse(params, cmd));
        channel.writeAndFlush(req);
        return promise;
    }

    @Override
    public Future<Boolean> exists(byte[] key) {
        return execCmd(booleanConverter, EXISTS, key);
    }

    @Override
    public Future<Boolean> expire(byte[] key, long seconds) {
        return execCmd(booleanConverter, EXPIRE, key, toBytes(seconds));
    }

    @Override
    public Future<Boolean> expireAt(byte[] key, long unixTimeSeconds) {
        return execCmd(booleanConverter, EXPIREAT, key, toBytes(unixTimeSeconds));
    }

    @Override
    public Future<Void> flushall() {
        return execCmd(voidConverter, FLUSHALL);
    }

    @Override
    public Future<Void> flushdb() {
        return execCmd(voidConverter, FLUSHDB);
    }

    @Override
    public Future<byte[]> get(byte[] key) {
        return execCmd(bytesConverter, GET, key);
    }

    @Override
    public Future<Boolean> getbit(byte[] key, long offset) {
        return execCmd(booleanConverter, GETBIT, key, toBytes(offset));
    }

    @Override
    public Future<byte[]> getrange(byte[] key, long startInclusive, long endInclusive) {
        return execCmd(bytesConverter, GETRANGE, key, toBytes(startInclusive),
                toBytes(endInclusive));
    }

    @Override
    public Future<byte[]> getset(byte[] key, byte[] value) {
        return execCmd(bytesConverter, GETSET, key, value);
    }

    @Override
    public Future<Long> hdel(byte[] key, byte[]... fields) {
        return execCmd(longConverter, HDEL, toParamsReverse(fields, key));
    }

    @Override
    public Future<Boolean> hexists(byte[] key, byte[] field) {
        return execCmd(booleanConverter, HEXISTS, key, field);
    }

    @Override
    public Future<byte[]> hget(byte[] key, byte[] field) {
        return execCmd(bytesConverter, HGET, key, field);
    }

    @Override
    public Future<Map<byte[], byte[]>> hgetAll(byte[] key) {
        return execCmd(mapConverter, HGETALL, key);
    }

    @Override
    public Future<Long> hincrby(byte[] key, byte[] field, long delta) {
        return execCmd(longConverter, HINCRBY, key, field, toBytes(delta));
    }

    @Override
    public Future<Double> hincrbyfloat(byte[] key, byte[] field, double delta) {
        return execCmd(doubleConverter, HINCRBYFLOAT, key, field, toBytes(delta));
    }

    @Override
    public Future<List<byte[]>> hkeys(byte[] key) {
        return execCmd(arrayConverter, HKEYS, key);
    }

    @Override
    public Future<Long> hlen(byte[] key) {
        return execCmd(longConverter, HLEN, key);
    }

    @Override
    public Future<List<byte[]>> hmget(byte[] key, byte[]... fields) {
        return execCmd(arrayConverter, HMGET, toParamsReverse(fields, key));
    }

    @Override
    public Future<Void> hmset(byte[] key, Map<byte[], byte[]> map) {
        byte[][] params = new byte[2 * map.size() + 1][];
        params[0] = key;
        int i = 1;
        for (Map.Entry<byte[], byte[]> e: map.entrySet()) {
            params[i++] = e.getKey();
            params[i++] = e.getValue();
        }
        return execCmd(voidConverter, HMSET, params);
    }

    @Override
    public Future<ScanResult<HashEntry>> hscan(byte[] key, ScanParams params) {
        List<byte[]> p = new ArrayList<>();
        p.add(key);
        addScanParams(p, params);
        return execCmd(hashScanResultConverter, HSCAN, p.toArray(new byte[0][]));
    }

    @Override
    public Future<Boolean> hset(byte[] key, byte[] field, byte[] value) {
        return execCmd(booleanConverter, HSET, key, field, value);
    }

    @Override
    public Future<Boolean> hsetnx(byte[] key, byte[] field, byte[] value) {
        return execCmd(booleanConverter, HSETNX, key, field, value);
    }

    @Override
    public Future<List<byte[]>> hvals(byte[] key) {
        return execCmd(arrayConverter, HVALS, key);
    }

    @Override
    public Future<Long> incr(byte[] key) {
        return execCmd(longConverter, INCR, key);
    }

    @Override
    public Future<Long> incrBy(byte[] key, long delta) {
        return execCmd(longConverter, INCRBY, key, toBytes(delta));
    }

    @Override
    public Future<Double> incrByFloat(byte[] key, double delta) {
        return execCmd(doubleConverter, INCRBYFLOAT, key, toBytes(delta));
    }

    @Override
    public Future<byte[]> info() {
        return execCmd(bytesConverter, INFO);
    }

    @Override
    public Future<byte[]> info(byte[] section) {
        return execCmd(bytesConverter, INFO, section);
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public Future<List<byte[]>> keys(byte[] pattern) {
        return execCmd(arrayConverter, KEYS, pattern);
    }

    @Override
    public Future<Long> lastsave() {
        return execCmd(longConverter, LASTSAVE);
    }

    @Override
    public Future<byte[]> lindex(byte[] key, long index) {
        return execCmd(bytesConverter, LINDEX, key, toBytes(index));
    }

    @Override
    public Future<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return execCmd(longConverter, LINSERT, key, where.raw, pivot, value);
    }

    @Override
    public Future<Long> llen(byte[] key) {
        return execCmd(longConverter, LLEN, key);
    }

    @Override
    public Future<byte[]> lpop(byte[] key) {
        return execCmd(bytesConverter, LPOP, key);
    }

    @Override
    public Future<Long> lpush(byte[] key, byte[]... values) {
        return execCmd(longConverter, LPUSH, toParamsReverse(values, key));
    }

    @Override
    public Future<Long> lpushx(byte[] key, byte[] value) {
        return execCmd(longConverter, LPUSHX, key, value);
    }

    @Override
    public Future<List<byte[]>> lrange(byte[] key, long startInclusive, long stopInclusive) {
        return execCmd(arrayConverter, LRANGE, key, toBytes(startInclusive), toBytes(stopInclusive));
    }

    @Override
    public Future<Long> lrem(byte[] key, long count, byte[] value) {
        return execCmd(longConverter, LREM, key, toBytes(count), value);
    }

    @Override
    public Future<byte[]> lset(byte[] key, long index, byte[] value) {
        return execCmd(bytesConverter, LSET, key, toBytes(index), value);
    }

    @Override
    public Future<Void> ltrim(byte[] key, long startInclusive, long stopInclusive) {
        return execCmd(voidConverter, LTRIM, key, toBytes(startInclusive), toBytes(stopInclusive));
    }

    @Override
    public Future<List<byte[]>> mget(byte[]... keys) {
        return execCmd(arrayConverter, MGET, keys);
    }

    @Override
    public Future<Void> migrate(byte[] host, int port, byte[] key, int dstDb, long timeoutMs) {
        return execCmd(voidConverter, host, toBytes(port), key, toBytes(dstDb), toBytes(timeoutMs));
    }

    @Override
    public Future<Boolean> move(byte[] key, int db) {
        return execCmd(booleanConverter, MOVE, key, toBytes(db));
    }

    @Override
    public Future<Void> mset(byte[]... keysvalues) {
        return execCmd(voidConverter, MSET, keysvalues);
    }

    @Override
    public Future<Boolean> msetnx(byte[]... keysvalues) {
        return execCmd(booleanConverter, MSETNX, keysvalues);
    }

    @Override
    public Future<Boolean> persist(byte[] key) {
        return execCmd(booleanConverter, PERSIST, key);
    }

    @Override
    public Future<Boolean> pexpire(byte[] key, long millis) {
        return execCmd(booleanConverter, PEXPIRE, toBytes(millis));
    }

    @Override
    public Future<Boolean> pexpireAt(byte[] key, long unixTimeMs) {
        return execCmd(booleanConverter, PEXPIREAT, toBytes(unixTimeMs));
    }

    @Override
    public Future<String> ping() {
        return execCmd(stringConverter, PING);
    }

    @Override
    public Future<Long> pttl(byte[] key) {
        return execCmd(longConverter, PTTL, key);
    }

    @Override
    public Future<Void> quit() {
        if (pool != null) {
            Promise<Void> promise = eventLoop().newPromise();
            promise.tryFailure(new OperationNotSupportedException(
                    "'quit' is not allowed on a pooled connection"));
            return promise;
        }
        return quit0();
    }

    Future<Void> quit0() {
        return execCmd(voidConverter, QUIT);
    }

    @Override
    public Future<byte[]> randomkey() {
        return execCmd(bytesConverter, RANDOMKEY);
    }

    @Override
    public void release() {
        if (pool != null && pool.exclusive()) {
            pool.release(this);
        }
    }

    @Override
    public Future<Void> rename(byte[] key, byte[] newKey) {
        return execCmd(voidConverter, RENAME, key, newKey);
    }

    @Override
    public Future<Boolean> renamenx(byte[] key, byte[] newKey) {
        return execCmd(booleanConverter, RENAMENX, key, newKey);
    }

    @Override
    public Future<Void> restore(byte[] key, int ttlMs, byte[] serializedValue, boolean replace) {
        if (replace) {
            return execCmd(voidConverter, RESTORE, key, toBytes(ttlMs), serializedValue,
                    REPLACE.raw);
        } else {
            return execCmd(voidConverter, RESTORE, key, toBytes(ttlMs), serializedValue);
        }
    }

    @Override
    public Future<List<byte[]>> role() {
        return execCmd(arrayConverter, ROLE);
    }

    @Override
    public Future<byte[]> rpop(byte[] key) {
        return execCmd(bytesConverter, RPOP, key);
    }

    @Override
    public Future<byte[]> rpoplpush(byte[] src, byte[] dst) {
        return execCmd(bytesConverter, RPOPLPUSH, src, dst);
    }

    @Override
    public Future<Long> rpush(byte[] key, byte[]... values) {
        return execCmd(longConverter, RPUSH, toParamsReverse(values, key));
    }

    @Override
    public Future<Long> rpushx(byte[] key, byte[] value) {
        return execCmd(longConverter, RPUSHX, key, value);
    }

    @Override
    public Future<Long> sadd(byte[] key, byte[]... members) {
        return execCmd(longConverter, SADD, toParamsReverse(members, key));
    }

    @Override
    public Future<Void> save(boolean save) {
        return execCmd(voidConverter, SAVE);
    }

    @Override
    public Future<ScanResult<byte[]>> scan(ScanParams params) {
        List<byte[]> p = new ArrayList<>();
        addScanParams(p, params);
        return execCmd(arrayScanResultConverter, SCAN, p.toArray(new byte[0][]));
    }

    @Override
    public Future<Long> scard(byte[] key) {
        return execCmd(longConverter, SCARD, key);
    }

    @Override
    public Future<List<byte[]>> sdiff(byte[]... keys) {
        return execCmd(arrayConverter, SDIFF, keys);
    }

    @Override
    public Future<Long> sdiffstore(byte[] dst, byte[]... keys) {
        return execCmd(longConverter, SDIFFSTORE, toParamsReverse(keys, dst));
    }

    @Override
    public Future<Void> select(int index) {
        if (pool != null) {
            Promise<Void> promise = eventLoop().newPromise();
            promise.tryFailure(new OperationNotSupportedException(
                    "'select' is not allowed on a pooled connection"));
            return promise;
        }
        return select0(index);
    }

    Future<Void> select0(int index) {
        return execCmd(voidConverter, SELECT, toBytes(index));
    }

    @Override
    public Future<Boolean> set(byte[] key, byte[] value) {
        return set(key, value, new SetParams());
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
        return execCmd(booleanConverter, SET, p.toArray(new byte[0][]));
    }

    @Override
    public Future<Boolean> setbit(byte[] key, long offset, boolean bit) {
        return execCmd(booleanConverter, SETBIT, key, toBytes(offset), toBytes(bit));
    }

    @Override
    public Future<Long> setrange(byte[] key, long offset, byte[] value) {
        return execCmd(longConverter, SETRANGE, key, toBytes(offset), value);
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
    public Future<List<byte[]>> sinter(byte[]... keys) {
        return execCmd(arrayConverter, SINTER, keys);
    }

    @Override
    public Future<Long> sinterstore(byte[] dst, byte[]... keys) {
        return execCmd(longConverter, SINTERSTORE, toParamsReverse(keys, dst));
    }

    @Override
    public Future<Boolean> sismember(byte[] key, byte[] member) {
        return execCmd(booleanConverter, SISMEMBER, key, member);
    }

    @Override
    public Future<Void> slaveof(String host, int port) {
        return execCmd(voidConverter, SLAVEOF, toBytes(host), toBytes(port));
    }

    @Override
    public Future<List<byte[]>> smembers(byte[] key) {
        return execCmd(arrayConverter, SMEMBERS, key);
    }

    @Override
    public Future<Boolean> smove(byte[] src, byte[] dst, byte[] member) {
        return execCmd(booleanConverter, SMOVE, src, dst, member);
    }

    @Override
    public Future<byte[]> spop(byte[] key) {
        return execCmd(bytesConverter, SPOP, key);
    }

    @Override
    public Future<byte[]> srandmember(byte[] key) {
        return execCmd(bytesConverter, SRANDMEMBER, key);
    }

    @Override
    public Future<List<byte[]>> srandmember(byte[] key, long count) {
        return execCmd(arrayConverter, SRANDMEMBER, key, toBytes(count));
    }

    @Override
    public Future<Long> srem(byte[] key, byte[]... members) {
        return execCmd(longConverter, SREM, toParamsReverse(members, key));
    }

    @Override
    public Future<ScanResult<byte[]>> sscan(byte[] key, ScanParams params) {
        List<byte[]> p = new ArrayList<>();
        p.add(key);
        addScanParams(p, params);
        return execCmd(arrayScanResultConverter, SSCAN, p.toArray(new byte[0][]));
    }

    @Override
    public Future<Long> strlen(byte[] key) {
        return execCmd(longConverter, STRLEN, key);
    }

    @Override
    public Future<List<byte[]>> sunion(byte[]... keys) {
        return execCmd(arrayConverter, SUNION, keys);
    }

    @Override
    public Future<Long> sunionstore(byte[] dst, byte[]... keys) {
        return execCmd(longConverter, SUNIONSTORE, toParamsReverse(keys, dst));
    }

    @Override
    public Future<Void> sync() {
        return execCmd(voidConverter, SYNC);
    }

    @Override
    public Future<List<byte[]>> time() {
        return execCmd(arrayConverter, TIME);
    }

    @Override
    public Future<Long> ttl(byte[] key) {
        return execCmd(longConverter, TTL, key);
    }

    @Override
    public Future<String> type(byte[] key) {
        return execCmd(stringConverter, TYPE, key);
    }
}
