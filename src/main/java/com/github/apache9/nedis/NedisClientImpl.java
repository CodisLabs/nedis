package com.github.apache9.nedis;

import static com.github.apache9.nedis.NedisUtils.toBytes;
import static com.github.apache9.nedis.NedisUtils.toParams;
import static com.github.apache9.nedis.NedisUtils.toParamsReverse;
import static com.github.apache9.nedis.protocol.RedisCommand.AUTH;
import static com.github.apache9.nedis.protocol.RedisCommand.BGREWRITEAOF;
import static com.github.apache9.nedis.protocol.RedisCommand.BGSAVE;
import static com.github.apache9.nedis.protocol.RedisCommand.BLPOP;
import static com.github.apache9.nedis.protocol.RedisCommand.BRPOP;
import static com.github.apache9.nedis.protocol.RedisCommand.BRPOPLPUSH;
import static com.github.apache9.nedis.protocol.RedisCommand.CLIENT;
import static com.github.apache9.nedis.protocol.RedisCommand.CONFIG;
import static com.github.apache9.nedis.protocol.RedisCommand.DBSIZE;
import static com.github.apache9.nedis.protocol.RedisCommand.DECR;
import static com.github.apache9.nedis.protocol.RedisCommand.DECRBY;
import static com.github.apache9.nedis.protocol.RedisCommand.DEL;
import static com.github.apache9.nedis.protocol.RedisCommand.DUMP;
import static com.github.apache9.nedis.protocol.RedisCommand.ECHO;
import static com.github.apache9.nedis.protocol.RedisCommand.EVAL;
import static com.github.apache9.nedis.protocol.RedisCommand.EXISTS;
import static com.github.apache9.nedis.protocol.RedisCommand.EXPIRE;
import static com.github.apache9.nedis.protocol.RedisCommand.EXPIREAT;
import static com.github.apache9.nedis.protocol.RedisCommand.FLUSHALL;
import static com.github.apache9.nedis.protocol.RedisCommand.FLUSHDB;
import static com.github.apache9.nedis.protocol.RedisCommand.GET;
import static com.github.apache9.nedis.protocol.RedisCommand.INCR;
import static com.github.apache9.nedis.protocol.RedisCommand.INCRBY;
import static com.github.apache9.nedis.protocol.RedisCommand.INCRBYFLOAT;
import static com.github.apache9.nedis.protocol.RedisCommand.INFO;
import static com.github.apache9.nedis.protocol.RedisCommand.KEYS;
import static com.github.apache9.nedis.protocol.RedisCommand.LASTSAVE;
import static com.github.apache9.nedis.protocol.RedisCommand.LINDEX;
import static com.github.apache9.nedis.protocol.RedisCommand.LINSERT;
import static com.github.apache9.nedis.protocol.RedisCommand.LLEN;
import static com.github.apache9.nedis.protocol.RedisCommand.LPOP;
import static com.github.apache9.nedis.protocol.RedisCommand.LPUSH;
import static com.github.apache9.nedis.protocol.RedisCommand.LPUSHX;
import static com.github.apache9.nedis.protocol.RedisCommand.LRANGE;
import static com.github.apache9.nedis.protocol.RedisCommand.LREM;
import static com.github.apache9.nedis.protocol.RedisCommand.LSET;
import static com.github.apache9.nedis.protocol.RedisCommand.LTRIM;
import static com.github.apache9.nedis.protocol.RedisCommand.MGET;
import static com.github.apache9.nedis.protocol.RedisCommand.MOVE;
import static com.github.apache9.nedis.protocol.RedisCommand.MSET;
import static com.github.apache9.nedis.protocol.RedisCommand.MSETNX;
import static com.github.apache9.nedis.protocol.RedisCommand.PERSIST;
import static com.github.apache9.nedis.protocol.RedisCommand.PEXPIRE;
import static com.github.apache9.nedis.protocol.RedisCommand.PEXPIREAT;
import static com.github.apache9.nedis.protocol.RedisCommand.PING;
import static com.github.apache9.nedis.protocol.RedisCommand.PTTL;
import static com.github.apache9.nedis.protocol.RedisCommand.QUIT;
import static com.github.apache9.nedis.protocol.RedisCommand.RANDOMKEY;
import static com.github.apache9.nedis.protocol.RedisCommand.RENAME;
import static com.github.apache9.nedis.protocol.RedisCommand.RENAMENX;
import static com.github.apache9.nedis.protocol.RedisCommand.RESTORE;
import static com.github.apache9.nedis.protocol.RedisCommand.ROLE;
import static com.github.apache9.nedis.protocol.RedisCommand.RPOP;
import static com.github.apache9.nedis.protocol.RedisCommand.RPOPLPUSH;
import static com.github.apache9.nedis.protocol.RedisCommand.RPUSH;
import static com.github.apache9.nedis.protocol.RedisCommand.RPUSHX;
import static com.github.apache9.nedis.protocol.RedisCommand.SADD;
import static com.github.apache9.nedis.protocol.RedisCommand.SAVE;
import static com.github.apache9.nedis.protocol.RedisCommand.SCARD;
import static com.github.apache9.nedis.protocol.RedisCommand.SDIFF;
import static com.github.apache9.nedis.protocol.RedisCommand.SDIFFSTORE;
import static com.github.apache9.nedis.protocol.RedisCommand.SELECT;
import static com.github.apache9.nedis.protocol.RedisCommand.SET;
import static com.github.apache9.nedis.protocol.RedisCommand.SINTER;
import static com.github.apache9.nedis.protocol.RedisCommand.SINTERSTORE;
import static com.github.apache9.nedis.protocol.RedisCommand.SISMEMBER;
import static com.github.apache9.nedis.protocol.RedisCommand.SLAVEOF;
import static com.github.apache9.nedis.protocol.RedisCommand.SMEMBERS;
import static com.github.apache9.nedis.protocol.RedisCommand.SMOVE;
import static com.github.apache9.nedis.protocol.RedisCommand.SPOP;
import static com.github.apache9.nedis.protocol.RedisCommand.SRANDMEMBER;
import static com.github.apache9.nedis.protocol.RedisCommand.SREM;
import static com.github.apache9.nedis.protocol.RedisCommand.SUNION;
import static com.github.apache9.nedis.protocol.RedisCommand.SUNIONSTORE;
import static com.github.apache9.nedis.protocol.RedisCommand.SYNC;
import static com.github.apache9.nedis.protocol.RedisCommand.TIME;
import static com.github.apache9.nedis.protocol.RedisCommand.TTL;
import static com.github.apache9.nedis.protocol.RedisCommand.TYPE;
import static com.github.apache9.nedis.protocol.RedisKeyword.EX;
import static com.github.apache9.nedis.protocol.RedisKeyword.GETNAME;
import static com.github.apache9.nedis.protocol.RedisKeyword.KILL;
import static com.github.apache9.nedis.protocol.RedisKeyword.LIST;
import static com.github.apache9.nedis.protocol.RedisKeyword.NX;
import static com.github.apache9.nedis.protocol.RedisKeyword.PX;
import static com.github.apache9.nedis.protocol.RedisKeyword.REPLACE;
import static com.github.apache9.nedis.protocol.RedisKeyword.RESETSTAT;
import static com.github.apache9.nedis.protocol.RedisKeyword.REWRITE;
import static com.github.apache9.nedis.protocol.RedisKeyword.SETNAME;
import static com.github.apache9.nedis.protocol.RedisKeyword.XX;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.naming.OperationNotSupportedException;

import com.github.apache9.nedis.handler.RedisDuplexHandler;
import com.github.apache9.nedis.protocol.RedisCommand;
import com.github.apache9.nedis.protocol.RedisKeyword;

/**
 * @author zhangduo
 */
public class NedisClientImpl implements NedisClient {

    private final Channel channel;

    private final NedisClientPool pool;

    private final PromiseConverter<List<byte[]>> arrayConverter;

    private final PromiseConverter<Boolean> booleanConverter;

    private final PromiseConverter<byte[]> bytesConverter;

    private final PromiseConverter<Double> doubleConverter;

    private final PromiseConverter<Long> longConverter;

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
    public Future<byte[]> get(byte[] key) {
        return execCmd(bytesConverter, GET, key);
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
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public Future<List<byte[]>> keys(byte[] pattern) {
        return execCmd(arrayConverter, KEYS, pattern);
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
    public Future<Long> sadd(byte[] key, byte[] member, byte[]... members) {
        return execCmd(longConverter, SADD, toParamsReverse(members, member));
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
        return execCmd(booleanConverter, SET, key, value);
    }

    @Override
    public Future<Boolean> setex(byte[] key, byte[] value, long seconds) {
        return execCmd(booleanConverter, SET, key, value, EX.raw, toBytes(seconds));
    }

    @Override
    public Future<Boolean> setexnx(byte[] key, byte[] value, long seconds) {
        return execCmd(booleanConverter, SET, key, value, EX.raw, toBytes(seconds), NX.raw);
    }

    @Override
    public Future<Boolean> setexxx(byte[] key, byte[] value, long seconds) {
        return execCmd(booleanConverter, SET, key, value, EX.raw, toBytes(seconds), XX.raw);
    }

    @Override
    public Future<Boolean> setnx(byte[] key, byte[] value) {
        return execCmd(booleanConverter, SET, key, value, NX.raw);
    }

    @Override
    public Future<Boolean> setpx(byte[] key, byte[] value, long milliseconds) {
        return execCmd(booleanConverter, SET, key, value, PX.raw, toBytes(milliseconds));
    }

    @Override
    public Future<Boolean> setpxnx(byte[] key, byte[] value, long milliseconds) {
        return execCmd(booleanConverter, SET, key, value, PX.raw, toBytes(milliseconds), NX.raw);
    }

    @Override
    public Future<Boolean> setpxxx(byte[] key, byte[] value, long milliseconds) {
        return execCmd(booleanConverter, SET, key, value, PX.raw, toBytes(milliseconds), XX.raw);
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
    public Future<Boolean> setxx(byte[] key, byte[] value) {
        return execCmd(booleanConverter, SET, key, value, XX.raw);
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
        return execCmd(longConverter, SREM, members);
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
    public Future<Long> ttl(byte[] key) {
        return execCmd(longConverter, TTL, key);
    }

    @Override
    public Future<String> type(byte[] key) {
        return execCmd(stringConverter, TYPE, key);
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
    public Future<Void> flushall() {
        return execCmd(voidConverter, FLUSHALL);
    }

    @Override
    public Future<Void> flushdb() {
        return execCmd(voidConverter, FLUSHDB);
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
    public Future<Long> lastsave() {
        return execCmd(longConverter, LASTSAVE);
    }

    @Override
    public Future<List<byte[]>> role() {
        return execCmd(arrayConverter, ROLE);
    }

    @Override
    public Future<Void> save(boolean save) {
        return execCmd(voidConverter, SAVE);
    }

    @Override
    public Future<Void> slaveof(String host, int port) {
        return execCmd(voidConverter, SLAVEOF, toBytes(host), toBytes(port));
    }

    @Override
    public Future<Void> sync() {
        return execCmd(voidConverter, SYNC);
    }

    @Override
    public Future<List<byte[]>> time() {
        return execCmd(arrayConverter, TIME);
    }
}
