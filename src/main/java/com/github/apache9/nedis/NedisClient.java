package com.github.apache9.nedis;

import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author zhangduo
 */
public interface NedisClient {

    Future<List<byte[]>> blpop(long timeoutSeconds, byte[]... keys);

    Future<List<byte[]>> brpop(long timeoutSeconds, byte[]... keys);

    Future<byte[]> brpoplpush(byte[] src, byte[] dst, long timeoutSeconds);

    Future<Long> decr(byte[] key);

    Future<Long> decrBy(byte[] key, long delta);

    Future<byte[]> echo(byte[] msg);

    Future<Long> del(byte[]... keys);

    Future<Object> eval(byte[] script, int numKeys, byte[]... keysvalues);

    Future<Boolean> exists(byte[] key);

    Future<Boolean> expire(byte[] key, long seconds);

    Future<Boolean> expireAt(byte[] key, long unixTime);

    Future<byte[]> get(byte[] key);

    Future<Long> incr(byte[] key);

    Future<Long> incrBy(byte[] key, long delta);

    Future<Double> incrByFloat(byte[] key, double delta);

    Future<byte[]> lindex(byte[] key, long index);

    public enum LIST_POSITION {
        BEFORE, AFTER;
        public final byte[] raw;

        private LIST_POSITION() {
            raw = name().getBytes(StandardCharsets.UTF_8);
        }
    }

    Future<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value);

    Future<Long> llen(byte[] key);

    Future<byte[]> lpop(byte[] key);

    Future<Long> lpush(byte[] key, byte[]... values);

    Future<Long> lpushx(byte[] key, byte[] value);

    Future<List<byte[]>> lrange(byte[] key, long startInclusive, long stopInclusive);

    Future<Long> lrem(byte[] key, long count, byte[] value);

    Future<byte[]> lset(byte[] key, long index, byte[] value);

    Future<Void> ltrim(byte[] key, long startInclusive, long stopInclusive);

    Future<List<byte[]>> mget(byte[]... keys);

    Future<Void> mset(byte[]... keysvalues);

    Future<Boolean> msetnx(byte[]... keysvalues);

    Future<String> ping();

    Future<Void> quit();

    Future<byte[]> rpop(byte[] key);

    Future<byte[]> rpoplpush(byte[] src, byte[] dst);

    Future<Long> rpush(byte[] key, byte[]... values);

    Future<Long> rpushx(byte[] key, byte[] value);

    Future<Boolean> set(byte[] key, byte[] value);

    Future<Boolean> setex(byte[] key, byte[] value, long seconds);

    Future<Boolean> setexnx(byte[] key, byte[] value, long seconds);

    Future<Boolean> setexxx(byte[] key, byte[] value, long seconds);

    Future<Boolean> setnx(byte[] key, byte[] value);

    Future<Boolean> setpx(byte[] key, byte[] value, long milliseconds);

    Future<Boolean> setpxnx(byte[] key, byte[] value, long milliseconds);

    Future<Boolean> setpxxx(byte[] key, byte[] value, long milliseconds);

    Future<Boolean> setxx(byte[] key, byte[] value);

    Future<Object> execCmd(byte[] cmd, byte[]... params);

    /**
     * return previous timeout value. Possible null if the client is already closed.
     */
    Future<Long> setTimeout(long timeoutMs);

    EventLoop eventLoop();

    boolean isOpen();

    ChannelFuture closeFuture();

    ChannelFuture close();
}
