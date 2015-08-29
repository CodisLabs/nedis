package com.github.apache9.nedis;

import static com.github.apache9.nedis.NedisUtils.toBytes;
import static com.github.apache9.nedis.NedisUtils.toParams;
import static com.github.apache9.nedis.NedisUtils.toParamsReverse;
import static com.github.apache9.nedis.protocol.RedisCommand.*;
import static com.github.apache9.nedis.protocol.RedisKeyword.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.naming.OperationNotSupportedException;

import com.github.apache9.nedis.handler.RedisDuplexHandler;
import com.github.apache9.nedis.handler.RedisResponseDecoder;
import com.github.apache9.nedis.protocol.RedisCommand;

/**
 * @author zhangduo
 */
public class NedisClientImpl implements NedisClient {

    private abstract class CmdExecutorFactory<T> {

        public abstract FutureListener<Object> newListener(Promise<T> promise);

        public Promise<T> newPromise() {
            return eventLoop().newPromise();
        }
    }

    private final CmdExecutorFactory<List<byte[]>> arrayReplyCmdExecutorFactory = new CmdExecutorFactory<List<byte[]>>() {

        @Override
        public FutureListener<Object> newListener(final Promise<List<byte[]>> promise) {
            return new FutureListener<Object>() {

                @SuppressWarnings("unchecked")
                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    if (future.isSuccess()) {
                        Object resp = future.getNow();
                        if (resp instanceof RedisResponseException) {
                            promise.tryFailure((RedisResponseException) resp);
                        } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                            promise.trySuccess(null);
                        } else {
                            promise.trySuccess((List<byte[]>) resp);
                        }
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }
            };
        }
    };

    private final CmdExecutorFactory<Boolean> booleanReplyCmdExecutorFactory = new CmdExecutorFactory<Boolean>() {

        @Override
        public FutureListener<Object> newListener(final Promise<Boolean> promise) {
            return new FutureListener<Object>() {

                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    if (future.isSuccess()) {
                        Object resp = future.getNow();
                        if (resp instanceof RedisResponseException) {
                            promise.tryFailure((RedisResponseException) resp);
                        } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                            promise.trySuccess(false);
                        } else if (resp instanceof String) {
                            promise.trySuccess(true);
                        } else {
                            promise.trySuccess(((Long) resp).intValue() != 0);
                        }
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }

            };
        }
    };

    private final CmdExecutorFactory<byte[]> bytesReplyCmdExecutorFactory = new CmdExecutorFactory<byte[]>() {

        @Override
        public FutureListener<Object> newListener(final Promise<byte[]> promise) {
            return new FutureListener<Object>() {

                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    if (future.isSuccess()) {
                        Object resp = future.getNow();
                        if (resp instanceof RedisResponseException) {
                            promise.tryFailure((RedisResponseException) resp);
                        } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                            promise.trySuccess(null);
                        } else {
                            promise.trySuccess((byte[]) resp);
                        }
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }
            };
        }
    };

    private final CmdExecutorFactory<Double> doubleReplyCmdExecutorFactory = new CmdExecutorFactory<Double>() {

        @Override
        public FutureListener<Object> newListener(final Promise<Double> promise) {
            return new FutureListener<Object>() {

                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    if (future.isSuccess()) {
                        Object resp = future.getNow();
                        if (resp instanceof RedisResponseException) {
                            promise.tryFailure((RedisResponseException) resp);
                        } else {
                            promise.trySuccess(Double.valueOf(resp.toString()));
                        }
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }
            };
        }
    };

    private final CmdExecutorFactory<Long> longReplyCmdExecutorFactory = new CmdExecutorFactory<Long>() {

        @Override
        public FutureListener<Object> newListener(final Promise<Long> promise) {
            return new FutureListener<Object>() {

                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    if (future.isSuccess()) {
                        Object resp = future.getNow();
                        if (resp instanceof RedisResponseException) {
                            promise.tryFailure((RedisResponseException) resp);
                        } else {
                            promise.trySuccess((Long) resp);
                        }
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }

            };
        }
    };

    private final CmdExecutorFactory<Object> objectReplyCmdExecutorFactory = new CmdExecutorFactory<Object>() {

        @Override
        public FutureListener<Object> newListener(final Promise<Object> promise) {
            return new FutureListener<Object>() {

                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    if (future.isSuccess()) {
                        Object resp = future.getNow();
                        if (resp instanceof RedisResponseException) {
                            promise.tryFailure((RedisResponseException) resp);
                        } else {
                            promise.trySuccess(resp);
                        }
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }

            };
        }

    };

    private final CmdExecutorFactory<String> stringReplyCmdExecutorFactory = new CmdExecutorFactory<String>() {

        @Override
        public FutureListener<Object> newListener(final Promise<String> promise) {
            return new FutureListener<Object>() {

                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    if (future.isSuccess()) {
                        Object resp = future.getNow();
                        if (resp instanceof RedisResponseException) {
                            promise.tryFailure((RedisResponseException) resp);
                        } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                            promise.trySuccess(null);
                        } else {
                            promise.trySuccess(resp.toString());
                        }
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }

            };
        }
    };

    private final CmdExecutorFactory<Void> voidReplyCmdExecutorFactory = new CmdExecutorFactory<Void>() {

        @Override
        public FutureListener<Object> newListener(final Promise<Void> promise) {
            return new FutureListener<Object>() {

                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    if (future.isSuccess()) {
                        Object resp = future.getNow();
                        if (resp instanceof RedisResponseException) {
                            promise.tryFailure((RedisResponseException) resp);
                        } else {
                            promise.trySuccess(null);
                        }
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }

            };
        }
    };

    private final Channel channel;

    private final NedisClientPool pool;

    public NedisClientImpl(Channel channel, NedisClientPool pool) {
        this.channel = channel;
        this.pool = pool;
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
        return execCmd(voidReplyCmdExecutorFactory, AUTH, password);
    }

    @Override
    public Future<List<byte[]>> blpop(long timeoutSeconds, byte[]... keys) {
        return execCmd(arrayReplyCmdExecutorFactory, BLPOP, toParams(keys, toBytes(timeoutSeconds)));
    }

    @Override
    public Future<List<byte[]>> brpop(long timeoutSeconds, byte[]... keys) {
        return execCmd(arrayReplyCmdExecutorFactory, BRPOP, toParams(keys, toBytes(timeoutSeconds)));
    }

    @Override
    public Future<byte[]> brpoplpush(byte[] src, byte[] dst, long timeoutSeconds) {
        return execCmd(bytesReplyCmdExecutorFactory, BRPOPLPUSH, src, dst, toBytes(timeoutSeconds));
    }

    @Override
    public Future<Void> clientSetname(byte[] name) {
        if (pool != null && !pool.exclusive()) {
            Promise<Void> promise = eventLoop().newPromise();
            promise.tryFailure(new OperationNotSupportedException(
                    "'client setname' is not allowed on a shared pooled connection"));
            return promise;
        }
        return clientSetname0(name);
    }

    Future<Void> clientSetname0(byte[] name) {
        return execCmd(voidReplyCmdExecutorFactory, CLIENT, SETNAME.raw, name);
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
        return execCmd(longReplyCmdExecutorFactory, DECR, key);
    }

    @Override
    public Future<Long> decrBy(byte[] key, long delta) {
        return execCmd(longReplyCmdExecutorFactory, DECRBY, key, toBytes(delta));
    }

    @Override
    public Future<Long> del(byte[]... keys) {
        return execCmd(longReplyCmdExecutorFactory, DEL, keys);
    }

    @Override
    public Future<byte[]> dump(byte[] key) {
        return execCmd(bytesReplyCmdExecutorFactory, DUMP, key);
    }

    @Override
    public Future<byte[]> echo(byte[] msg) {
        return execCmd(bytesReplyCmdExecutorFactory, ECHO, msg);
    }

    @Override
    public Future<Object> eval(byte[] script, int numKeys, byte[]... keysvalues) {
        return execCmd(objectReplyCmdExecutorFactory, EVAL,
                toParamsReverse(keysvalues, script, toBytes(numKeys)));
    }

    @Override
    public EventLoop eventLoop() {
        return channel.eventLoop();
    }

    @Override
    public Future<Object> execCmd(byte[] cmd, byte[]... params) {
        return execCmd(objectReplyCmdExecutorFactory, cmd, params);
    }

    private <T> Future<T> execCmd(CmdExecutorFactory<T> factory, byte[] cmd, byte[]... params) {
        Promise<T> promise = factory.newPromise();
        execCmd0(cmd, params).addListener(factory.newListener(promise));
        return promise;
    }

    private <T> Future<T> execCmd(CmdExecutorFactory<T> factory, RedisCommand cmd, byte[]... params) {
        return execCmd(factory, cmd.raw, params);
    }

    private Future<Object> execCmd0(byte[] cmd, byte[]... params) {
        Promise<Object> promise = eventLoop().newPromise();
        RedisRequest req = new RedisRequest(promise, toParamsReverse(params, cmd));
        channel.writeAndFlush(req);
        return promise;
    }

    @Override
    public Future<Boolean> exists(byte[] key) {
        return execCmd(booleanReplyCmdExecutorFactory, EXISTS, key);
    }

    @Override
    public Future<Boolean> expire(byte[] key, long seconds) {
        return execCmd(booleanReplyCmdExecutorFactory, EXPIRE, key, toBytes(seconds));
    }

    @Override
    public Future<Boolean> expireAt(byte[] key, long unixTimeSeconds) {
        return execCmd(booleanReplyCmdExecutorFactory, EXPIREAT, key, toBytes(unixTimeSeconds));
    }

    @Override
    public Future<byte[]> get(byte[] key) {
        return execCmd(bytesReplyCmdExecutorFactory, GET, key);
    }

    @Override
    public Future<Long> incr(byte[] key) {
        return execCmd(longReplyCmdExecutorFactory, INCR, key);
    }

    @Override
    public Future<Long> incrBy(byte[] key, long delta) {
        return execCmd(longReplyCmdExecutorFactory, INCRBY, key, toBytes(delta));
    }

    @Override
    public Future<Double> incrByFloat(byte[] key, double delta) {
        return execCmd(doubleReplyCmdExecutorFactory, INCRBYFLOAT, key, toBytes(delta));
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public Future<List<byte[]>> keys(byte[] pattern) {
        return execCmd(arrayReplyCmdExecutorFactory, KEYS, pattern);
    }

    @Override
    public Future<byte[]> lindex(byte[] key, long index) {
        return execCmd(bytesReplyCmdExecutorFactory, LINDEX, key, toBytes(index));
    }

    @Override
    public Future<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return execCmd(longReplyCmdExecutorFactory, LINSERT, key, where.raw, pivot, value);
    }

    @Override
    public Future<Long> llen(byte[] key) {
        return execCmd(longReplyCmdExecutorFactory, LLEN, key);
    }

    @Override
    public Future<byte[]> lpop(byte[] key) {
        return execCmd(bytesReplyCmdExecutorFactory, LPOP, key);
    }

    @Override
    public Future<Long> lpush(byte[] key, byte[]... values) {
        return execCmd(longReplyCmdExecutorFactory, LPUSH, toParamsReverse(values, key));
    }

    @Override
    public Future<Long> lpushx(byte[] key, byte[] value) {
        return execCmd(longReplyCmdExecutorFactory, LPUSHX, key, value);
    }

    @Override
    public Future<List<byte[]>> lrange(byte[] key, long startInclusive, long stopInclusive) {
        return execCmd(arrayReplyCmdExecutorFactory, LRANGE, key, toBytes(startInclusive),
                toBytes(stopInclusive));
    }

    @Override
    public Future<Long> lrem(byte[] key, long count, byte[] value) {
        return execCmd(longReplyCmdExecutorFactory, LREM, key, toBytes(count), value);
    }

    @Override
    public Future<byte[]> lset(byte[] key, long index, byte[] value) {
        return execCmd(bytesReplyCmdExecutorFactory, LSET, key, toBytes(index), value);
    }

    @Override
    public Future<Void> ltrim(byte[] key, long startInclusive, long stopInclusive) {
        return execCmd(voidReplyCmdExecutorFactory, LTRIM, key, toBytes(startInclusive),
                toBytes(stopInclusive));
    }

    @Override
    public Future<List<byte[]>> mget(byte[]... keys) {
        return execCmd(arrayReplyCmdExecutorFactory, MGET, keys);
    }

    @Override
    public Future<Void> migrate(byte[] host, int port, byte[] key, int dstDb, long timeoutMs) {
        return execCmd(voidReplyCmdExecutorFactory, host, toBytes(port), key, toBytes(dstDb),
                toBytes(timeoutMs));
    }

    @Override
    public Future<Boolean> move(byte[] key, int db) {
        return execCmd(booleanReplyCmdExecutorFactory, MOVE, key, toBytes(db));
    }

    @Override
    public Future<Void> mset(byte[]... keysvalues) {
        return execCmd(voidReplyCmdExecutorFactory, MSET, keysvalues);
    }

    @Override
    public Future<Boolean> msetnx(byte[]... keysvalues) {
        return execCmd(booleanReplyCmdExecutorFactory, MSETNX, keysvalues);
    }

    @Override
    public Future<Boolean> persist(byte[] key) {
        return execCmd(booleanReplyCmdExecutorFactory, PERSIST, key);
    }

    @Override
    public Future<Boolean> pexpire(byte[] key, long millis) {
        return execCmd(booleanReplyCmdExecutorFactory, PEXPIRE, toBytes(millis));
    }

    @Override
    public Future<Boolean> pexpireAt(byte[] key, long unixTimeMs) {
        return execCmd(booleanReplyCmdExecutorFactory, PEXPIREAT, toBytes(unixTimeMs));
    }

    @Override
    public Future<String> ping() {
        return execCmd(stringReplyCmdExecutorFactory, PING);
    }

    @Override
    public Future<Long> pttl(byte[] key) {
        return execCmd(longReplyCmdExecutorFactory, PTTL, key);
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
        return execCmd(voidReplyCmdExecutorFactory, QUIT);
    }

    @Override
    public Future<byte[]> randomkey() {
        return execCmd(bytesReplyCmdExecutorFactory, RANDOMKEY);
    }

    @Override
    public void release() {
        if (pool != null && pool.exclusive()) {
            pool.release(this);
        }
    }

    @Override
    public Future<Void> rename(byte[] key, byte[] newKey) {
        return execCmd(voidReplyCmdExecutorFactory, RENAME, key, newKey);
    }

    @Override
    public Future<Boolean> renamenx(byte[] key, byte[] newKey) {
        return execCmd(booleanReplyCmdExecutorFactory, RENAMENX, key, newKey);
    }

    @Override
    public Future<Void> restore(byte[] key, int ttlMs, byte[] serializedValue, boolean replace) {
        if (replace) {
            return execCmd(voidReplyCmdExecutorFactory, RESTORE, key, toBytes(ttlMs),
                    serializedValue, REPLACE.raw);
        } else {
            return execCmd(voidReplyCmdExecutorFactory, RESTORE, key, toBytes(ttlMs),
                    serializedValue);
        }
    }

    @Override
    public Future<byte[]> rpop(byte[] key) {
        return execCmd(bytesReplyCmdExecutorFactory, RPOP, key);
    }

    @Override
    public Future<byte[]> rpoplpush(byte[] src, byte[] dst) {
        return execCmd(bytesReplyCmdExecutorFactory, RPOPLPUSH, src, dst);
    }

    @Override
    public Future<Long> rpush(byte[] key, byte[]... values) {
        return execCmd(longReplyCmdExecutorFactory, RPUSH, toParamsReverse(values, key));
    }

    @Override
    public Future<Long> rpushx(byte[] key, byte[] value) {
        return execCmd(longReplyCmdExecutorFactory, RPUSHX, key, value);
    }

    @Override
    public Future<Long> sadd(byte[] key, byte[] member, byte[]... members) {
        return execCmd(longReplyCmdExecutorFactory, SADD, toParamsReverse(members, member));
    }

    @Override
    public Future<Long> scard(byte[] key) {
        return execCmd(longReplyCmdExecutorFactory, SCARD, key);
    }

    @Override
    public Future<List<byte[]>> sdiff(byte[]... keys) {
        return execCmd(arrayReplyCmdExecutorFactory, SDIFF, keys);
    }

    @Override
    public Future<Long> sdiffstore(byte[] dst, byte[]... keys) {
        return execCmd(longReplyCmdExecutorFactory, SDIFFSTORE, toParamsReverse(keys, dst));
    }

    @Override
    public Future<Void> select(int index) {
        if (pool != null && !pool.exclusive()) {
            Promise<Void> promise = eventLoop().newPromise();
            promise.tryFailure(new OperationNotSupportedException(
                    "'select' is not allowed on a shared pooled connection"));
            return promise;
        }
        return select0(index);
    }

    Future<Void> select0(int index) {
        return execCmd(voidReplyCmdExecutorFactory, SELECT, toBytes(index));
    }

    @Override
    public Future<Boolean> set(byte[] key, byte[] value) {
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value);
    }

    @Override
    public Future<Boolean> setex(byte[] key, byte[] value, long seconds) {
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value, EX.raw, toBytes(seconds));
    }

    @Override
    public Future<Boolean> setexnx(byte[] key, byte[] value, long seconds) {
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value, EX.raw, toBytes(seconds),
                NX.raw);
    }

    @Override
    public Future<Boolean> setexxx(byte[] key, byte[] value, long seconds) {
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value, EX.raw, toBytes(seconds),
                XX.raw);
    }

    @Override
    public Future<Boolean> setnx(byte[] key, byte[] value) {
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value, NX.raw);
    }

    @Override
    public Future<Boolean> setpx(byte[] key, byte[] value, long milliseconds) {
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value, PX.raw,
                toBytes(milliseconds));
    }

    @Override
    public Future<Boolean> setpxnx(byte[] key, byte[] value, long milliseconds) {
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value, PX.raw,
                toBytes(milliseconds), NX.raw);
    }

    @Override
    public Future<Boolean> setpxxx(byte[] key, byte[] value, long milliseconds) {
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value, PX.raw,
                toBytes(milliseconds), XX.raw);
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
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value, XX.raw);
    }

    @Override
    public Future<List<byte[]>> sinter(byte[]... keys) {
        return execCmd(arrayReplyCmdExecutorFactory, SINTER, keys);
    }

    @Override
    public Future<Long> sinterstore(byte[] dst, byte[]... keys) {
        return execCmd(longReplyCmdExecutorFactory, SINTERSTORE, toParamsReverse(keys, dst));
    }

    @Override
    public Future<Boolean> sismember(byte[] key, byte[] member) {
        return execCmd(booleanReplyCmdExecutorFactory, SISMEMBER, key, member);
    }

    @Override
    public Future<List<byte[]>> smembers(byte[] key) {
        return execCmd(arrayReplyCmdExecutorFactory, SMEMBERS, key);
    }

    @Override
    public Future<Boolean> smove(byte[] src, byte[] dst, byte[] member) {
        return execCmd(booleanReplyCmdExecutorFactory, SMOVE, src, dst, member);
    }

    @Override
    public Future<byte[]> spop(byte[] key) {
        return execCmd(bytesReplyCmdExecutorFactory, SPOP, key);
    }

    @Override
    public Future<byte[]> srandmember(byte[] key) {
        return execCmd(bytesReplyCmdExecutorFactory, SRANDMEMBER, key);
    }

    @Override
    public Future<List<byte[]>> srandmember(byte[] key, long count) {
        return execCmd(arrayReplyCmdExecutorFactory, SRANDMEMBER, key, toBytes(count));
    }

    @Override
    public Future<Long> srem(byte[] key, byte[]... members) {
        return execCmd(longReplyCmdExecutorFactory, SREM, members);
    }

    @Override
    public Future<List<byte[]>> sunion(byte[]... keys) {
        return execCmd(arrayReplyCmdExecutorFactory, SUNION, keys);
    }

    @Override
    public Future<Long> sunionstore(byte[] dst, byte[]... keys) {
        return execCmd(longReplyCmdExecutorFactory, SUNIONSTORE, toParamsReverse(keys, dst));
    }

    @Override
    public Future<Long> ttl(byte[] key) {
        return execCmd(longReplyCmdExecutorFactory, TTL, key);
    }

    @Override
    public Future<String> type(byte[] key) {
        return execCmd(stringReplyCmdExecutorFactory, TYPE, key);
    }
}
