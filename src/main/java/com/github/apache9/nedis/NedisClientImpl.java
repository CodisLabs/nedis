package com.github.apache9.nedis;

import static com.github.apache9.nedis.RedisCommand.*;
import static com.github.apache9.nedis.RedisKeyword.EX;
import static com.github.apache9.nedis.RedisKeyword.NX;
import static com.github.apache9.nedis.RedisKeyword.PX;
import static com.github.apache9.nedis.RedisKeyword.XX;
import static com.github.apache9.nedis.NedisUtils.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.util.List;

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

    private final Channel conn;

    public NedisClientImpl(Channel conn) {
        this.conn = conn;
    }

    @Override
    public Future<Long> decr(byte[] key) {
        Promise<Long> promise = conn.eventLoop().newPromise();
        execCmd(DECR.raw, key).addListener(new LongFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Long> decrBy(byte[] key, long delta) {
        Promise<Long> promise = conn.eventLoop().newPromise();
        execCmd(DECRBY.raw, key, toBytes(delta)).addListener(new LongFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Long> del(byte[]... keys) {
        Promise<Long> promise = conn.eventLoop().newPromise();
        execCmd(DEL.raw, keys).addListener(new LongFutureListener(promise));
        return promise;
    }

    @Override
    public Future<byte[]> echo(byte[] msg) {
        Promise<byte[]> promise = conn.eventLoop().newPromise();
        execCmd(ECHO.raw, msg).addListener(new BytesFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Object> eval(byte[] script, int numKeys, byte[]... keysvalues) {
        return execCmd(EVAL.raw, toParams(keysvalues, script, toBytes(numKeys)));
    }

    @Override
    public Future<Boolean> exists(byte[] key) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(EXISTS.raw, key).addListener(new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> expire(byte[] key, long seconds) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(EXPIRE.raw, key, toBytes(seconds)).addListener(new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> expireAt(byte[] key, long unixTime) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(EXPIREAT.raw, key, toBytes(unixTime)).addListener(
                new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<byte[]> get(byte[] key) {
        Promise<byte[]> promise = conn.eventLoop().newPromise();
        execCmd(GET.raw, key).addListener(new BytesFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Long> incr(byte[] key) {
        Promise<Long> promise = conn.eventLoop().newPromise();
        execCmd(INCR.raw, key).addListener(new LongFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Long> incrBy(byte[] key, long delta) {
        Promise<Long> promise = conn.eventLoop().newPromise();
        execCmd(INCRBY.raw, key, toBytes(delta)).addListener(new LongFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Double> incrByFloat(byte[] key, double delta) {
        Promise<Double> promise = conn.eventLoop().newPromise();
        execCmd(INCRBYFLOAT.raw, key, toBytes(delta))
                .addListener(new DoubleFutureListener(promise));
        return promise;
    }

    @Override
    public Future<List<byte[]>> mget(byte[]... keys) {
        Promise<List<byte[]>> promise = conn.eventLoop().newPromise();
        execCmd(MGET.raw, keys).addListener(new ArrayBytesFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Void> mset(byte[]... keysvalues) {
        Promise<Void> promise = conn.eventLoop().newPromise();
        execCmd(MSET.raw, keysvalues).addListener(new VoidFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> msetnx(byte[]... keysvalues) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(MSETNX.raw, keysvalues).addListener(new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<String> ping() {
        Promise<String> promise = conn.eventLoop().newPromise();
        execCmd(PING.raw).addListener(new StringFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Void> quit() {
        Promise<Void> promise = conn.eventLoop().newPromise();
        execCmd(QUIT.raw).addListener(new VoidFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> set(byte[] key, byte[] value) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(SET.raw, key, value).addListener(new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> setex(byte[] key, byte[] value, long seconds) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(SET.raw, key, value, EX.raw, toBytes(seconds)).addListener(
                new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> setexnx(byte[] key, byte[] value, long seconds) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(SET.raw, key, value, EX.raw, toBytes(seconds), NX.raw).addListener(
                new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> setexxx(byte[] key, byte[] value, long seconds) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(SET.raw, key, value, EX.raw, toBytes(seconds), XX.raw).addListener(
                new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> setnx(byte[] key, byte[] value) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(SET.raw, key, value, NX.raw).addListener(new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> setpx(byte[] key, byte[] value, long milliseconds) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(SET.raw, key, value, PX.raw, toBytes(milliseconds)).addListener(
                new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> setpxnx(byte[] key, byte[] value, long milliseconds) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(SET.raw, key, value, PX.raw, toBytes(milliseconds), NX.raw).addListener(
                new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> setpxxx(byte[] key, byte[] value, long milliseconds) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(SET.raw, key, value, PX.raw, toBytes(milliseconds), XX.raw).addListener(
                new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Boolean> setxx(byte[] key, byte[] value) {
        Promise<Boolean> promise = conn.eventLoop().newPromise();
        execCmd(SET.raw, key, value, XX.raw).addListener(new BooleanFutureListener(promise));
        return promise;
    }

    @Override
    public Future<Object> execCmd(byte[] cmd, byte[]... params) {
        Promise<Object> promise = conn.eventLoop().newPromise();
        RedisRequest req = new RedisRequest(promise, toParams(params, cmd));
        conn.writeAndFlush(req);
        return promise;
    }

    @Override
    public ChannelFuture close() {
        return conn.close();
    }
}
