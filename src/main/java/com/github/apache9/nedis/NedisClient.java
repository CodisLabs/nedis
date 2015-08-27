package com.github.apache9.nedis;

import java.util.List;

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;

/**
 * @author zhangduo
 */
public interface NedisClient {

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

    Future<List<byte[]>> mget(byte[]... keys);

    Future<Void> mset(byte[]... keysvalues);

    Future<Boolean> msetnx(byte[]... keysvalues);

    Future<String> ping();

    Future<Void> quit();

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

    ChannelFuture close();
}
