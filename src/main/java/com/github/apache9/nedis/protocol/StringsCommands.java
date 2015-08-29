package com.github.apache9.nedis.protocol;

import java.util.List;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface StringsCommands {

    Future<Long> decr(byte[] key);

    Future<Long> decrBy(byte[] key, long delta);

    Future<byte[]> get(byte[] key);

    Future<Long> incr(byte[] key);

    Future<Long> incrBy(byte[] key, long delta);

    Future<Double> incrByFloat(byte[] key, double delta);

    Future<List<byte[]>> mget(byte[]... keys);

    Future<Void> mset(byte[]... keysvalues);

    Future<Boolean> msetnx(byte[]... keysvalues);

    Future<Boolean> set(byte[] key, byte[] value);

    Future<Boolean> setex(byte[] key, byte[] value, long seconds);

    Future<Boolean> setexnx(byte[] key, byte[] value, long seconds);

    Future<Boolean> setexxx(byte[] key, byte[] value, long seconds);

    Future<Boolean> setnx(byte[] key, byte[] value);

    Future<Boolean> setpx(byte[] key, byte[] value, long milliseconds);

    Future<Boolean> setpxnx(byte[] key, byte[] value, long milliseconds);

    Future<Boolean> setpxxx(byte[] key, byte[] value, long milliseconds);

    Future<Boolean> setxx(byte[] key, byte[] value);
}
