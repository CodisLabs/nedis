package com.github.apache9.nedis;

import static com.github.apache9.nedis.NedisUtils.toBytes;
import static com.github.apache9.nedis.NedisUtils.toParams;
import static com.github.apache9.nedis.NedisUtils.toParamsReverse;
import static com.github.apache9.nedis.RedisCommand.BLPOP;
import static com.github.apache9.nedis.RedisCommand.BRPOP;
import static com.github.apache9.nedis.RedisCommand.BRPOPLPUSH;
import static com.github.apache9.nedis.RedisCommand.DECR;
import static com.github.apache9.nedis.RedisCommand.DECRBY;
import static com.github.apache9.nedis.RedisCommand.DEL;
import static com.github.apache9.nedis.RedisCommand.ECHO;
import static com.github.apache9.nedis.RedisCommand.EVAL;
import static com.github.apache9.nedis.RedisCommand.EXISTS;
import static com.github.apache9.nedis.RedisCommand.EXPIRE;
import static com.github.apache9.nedis.RedisCommand.EXPIREAT;
import static com.github.apache9.nedis.RedisCommand.GET;
import static com.github.apache9.nedis.RedisCommand.INCR;
import static com.github.apache9.nedis.RedisCommand.INCRBY;
import static com.github.apache9.nedis.RedisCommand.INCRBYFLOAT;
import static com.github.apache9.nedis.RedisCommand.LINDEX;
import static com.github.apache9.nedis.RedisCommand.LINSERT;
import static com.github.apache9.nedis.RedisCommand.LLEN;
import static com.github.apache9.nedis.RedisCommand.LPOP;
import static com.github.apache9.nedis.RedisCommand.LPUSH;
import static com.github.apache9.nedis.RedisCommand.LPUSHX;
import static com.github.apache9.nedis.RedisCommand.LRANGE;
import static com.github.apache9.nedis.RedisCommand.LREM;
import static com.github.apache9.nedis.RedisCommand.LSET;
import static com.github.apache9.nedis.RedisCommand.LTRIM;
import static com.github.apache9.nedis.RedisCommand.MGET;
import static com.github.apache9.nedis.RedisCommand.MSET;
import static com.github.apache9.nedis.RedisCommand.MSETNX;
import static com.github.apache9.nedis.RedisCommand.PING;
import static com.github.apache9.nedis.RedisCommand.QUIT;
import static com.github.apache9.nedis.RedisCommand.RPOP;
import static com.github.apache9.nedis.RedisCommand.RPOPLPUSH;
import static com.github.apache9.nedis.RedisCommand.RPUSH;
import static com.github.apache9.nedis.RedisCommand.RPUSHX;
import static com.github.apache9.nedis.RedisCommand.SET;
import static com.github.apache9.nedis.RedisKeyword.EX;
import static com.github.apache9.nedis.RedisKeyword.NX;
import static com.github.apache9.nedis.RedisKeyword.PX;
import static com.github.apache9.nedis.RedisKeyword.XX;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author zhangduo
 */
public class NedisClientImpl implements NedisClient {

    private static final class ArrayBytesFutureListener implements FutureListener<Object> {

        private final Promise<List<byte[]>> promise;

        public ArrayBytesFutureListener(Promise<List<byte[]>> promise) {
            this.promise = promise;
        }

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
    }

    private static final class BooleanFutureListener implements FutureListener<Object> {

        private final Promise<Boolean> promise;

        public BooleanFutureListener(Promise<Boolean> promise) {
            this.promise = promise;
        }

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
    }

    private static final class BytesFutureListener implements FutureListener<Object> {

        private final Promise<byte[]> promise;

        public BytesFutureListener(Promise<byte[]> promise) {
            this.promise = promise;
        }

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
    }

    private static final class DoubleFutureListener implements FutureListener<Object> {

        private final Promise<Double> promise;

        public DoubleFutureListener(Promise<Double> promise) {
            this.promise = promise;
        }

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
    }

    private static final class LongFutureListener implements FutureListener<Object> {

        private final Promise<Long> promise;

        public LongFutureListener(Promise<Long> promise) {
            this.promise = promise;
        }

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
    }

    private static final class StringFutureListener implements FutureListener<Object> {

        private final Promise<String> promise;

        public StringFutureListener(Promise<String> promise) {
            this.promise = promise;
        }

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
    }

    private static final class VoidFutureListener implements FutureListener<Object> {

        private final Promise<Void> promise;

        public VoidFutureListener(Promise<Void> promise) {
            this.promise = promise;
        }

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
    }

    private abstract class CmdExecutorFactory<T> {

        public Promise<T> newPromise() {
            return eventLoop().newPromise();
        }

        public abstract FutureListener<Object> newListener(Promise<T> promise);
    }

    private final CmdExecutorFactory<List<byte[]>> arrayReplyCmdExecutorFactory = new CmdExecutorFactory<List<byte[]>>() {

        @Override
        public FutureListener<Object> newListener(Promise<List<byte[]>> promise) {
            return new ArrayBytesFutureListener(promise);
        }
    };

    private final CmdExecutorFactory<Boolean> booleanReplyCmdExecutorFactory = new CmdExecutorFactory<Boolean>() {

        @Override
        public FutureListener<Object> newListener(Promise<Boolean> promise) {
            return new BooleanFutureListener(promise);
        }
    };

    private final CmdExecutorFactory<byte[]> bytesReplyCmdExecutorFactory = new CmdExecutorFactory<byte[]>() {

        @Override
        public FutureListener<Object> newListener(Promise<byte[]> promise) {
            return new BytesFutureListener(promise);
        }
    };

    private final CmdExecutorFactory<Double> doubleReplyCmdExecutorFactory = new CmdExecutorFactory<Double>() {

        @Override
        public FutureListener<Object> newListener(Promise<Double> promise) {
            return new DoubleFutureListener(promise);
        }
    };

    private final CmdExecutorFactory<Long> longReplyCmdExecutorFactory = new CmdExecutorFactory<Long>() {

        @Override
        public FutureListener<Object> newListener(Promise<Long> promise) {
            return new LongFutureListener(promise);
        }
    };

    private final CmdExecutorFactory<String> stringReplyCmdExecutorFactory = new CmdExecutorFactory<String>() {

        @Override
        public FutureListener<Object> newListener(Promise<String> promise) {
            return new StringFutureListener(promise);
        }
    };

    private final CmdExecutorFactory<Void> voidReplyCmdExecutorFactory = new CmdExecutorFactory<Void>() {

        @Override
        public FutureListener<Object> newListener(Promise<Void> promise) {
            return new VoidFutureListener(promise);
        }
    };

    private final Channel conn;

    public NedisClientImpl(Channel conn) {
        this.conn = conn;
    }

    private <T> Future<T> execCmd(CmdExecutorFactory<T> factory, RedisCommand cmd, byte[]... params) {
        Promise<T> promise = factory.newPromise();
        execCmd(cmd.raw, params).addListener(factory.newListener(promise));
        return promise;
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
    public Future<byte[]> echo(byte[] msg) {
        return execCmd(bytesReplyCmdExecutorFactory, ECHO, msg);
    }

    @Override
    public Future<Object> eval(byte[] script, int numKeys, byte[]... keysvalues) {
        return execCmd(EVAL.raw, toParamsReverse(keysvalues, script, toBytes(numKeys)));
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
    public Future<Boolean> expireAt(byte[] key, long unixTime) {
        return execCmd(booleanReplyCmdExecutorFactory, EXPIREAT, key, toBytes(unixTime));
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
    public Future<Void> mset(byte[]... keysvalues) {
        return execCmd(voidReplyCmdExecutorFactory, MSET, keysvalues);
    }

    @Override
    public Future<Boolean> msetnx(byte[]... keysvalues) {
        return execCmd(booleanReplyCmdExecutorFactory, MSETNX, keysvalues);
    }

    @Override
    public Future<String> ping() {
        return execCmd(stringReplyCmdExecutorFactory, PING);
    }

    @Override
    public Future<Void> quit() {
        return execCmd(voidReplyCmdExecutorFactory, QUIT);
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
    public Future<Boolean> setxx(byte[] key, byte[] value) {
        return execCmd(booleanReplyCmdExecutorFactory, SET, key, value, XX.raw);
    }

    @Override
    public Future<Object> execCmd(byte[] cmd, byte[]... params) {
        Promise<Object> promise = eventLoop().newPromise();
        RedisRequest req = new RedisRequest(promise, toParamsReverse(params, cmd));
        conn.writeAndFlush(req);
        return promise;
    }

    @Override
    public EventLoop eventLoop() {
        return conn.eventLoop();
    }

    @Override
    public boolean isOpen() {
        return conn.isOpen();
    }

    @Override
    public Future<Long> setTimeout(final long timeoutMs) {
        return eventLoop().submit(new Callable<Long>() {

            @Override
            public Long call() {
                RedisDuplexHandler handler = conn.pipeline().get(RedisDuplexHandler.class);
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
    public ChannelFuture close() {
        return conn.close();
    }

    @Override
    public ChannelFuture closeFuture() {
        return conn.closeFuture();
    }
}
