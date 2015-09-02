package com.wandoulabs.nedis.protocol;

import java.util.List;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface StringsCommands {

    Future<Long> append(byte[] key, byte[] value);

    Future<Long> bitcount(byte[] key);

    Future<Long> bitcount(byte[] key, long startInclusive, long endInclusive);

    Future<Long> bitop(BitOp op, byte[] dst, byte[]... keys);

    Future<Long> bitpos(byte[] key, boolean bit);

    Future<Long> bitpos(byte[] key, boolean bit, long startInclusive);

    Future<Long> bitpos(byte[] key, boolean bit, long startInclusive, long endInclusive);

    Future<Long> decr(byte[] key);

    Future<Long> decrBy(byte[] key, long delta);

    Future<byte[]> get(byte[] key);

    Future<Boolean> getbit(byte[] key, long offset);

    Future<byte[]> getrange(byte[] key, long startInclusive, long endInclusive);

    Future<byte[]> getset(byte[] key, byte[] value);

    Future<Long> incr(byte[] key);

    Future<Long> incrBy(byte[] key, long delta);

    Future<Double> incrByFloat(byte[] key, double delta);

    Future<List<byte[]>> mget(byte[]... keys);

    Future<Void> mset(byte[]... keysvalues);

    Future<Boolean> msetnx(byte[]... keysvalues);
    
    Future<Boolean> set(byte[] key, byte[] value);

    Future<Boolean> set(byte[] key, byte[] value, SetParams params);

    Future<Boolean> setbit(byte[] key, long offset, boolean bit);

    Future<Long> setrange(byte[] key, long offset, byte[] value);

    Future<Long> strlen(byte[] key);
}
