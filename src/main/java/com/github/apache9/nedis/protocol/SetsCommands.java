package com.github.apache9.nedis.protocol;

import java.util.List;

import io.netty.util.concurrent.Future;

/**
 * @author zhangduo
 */
public interface SetsCommands {

    Future<Long> sadd(byte[] key, byte[] member, byte[]... members);

    Future<Long> scard(byte[] key);

    Future<List<byte[]>> sdiff(byte[] key, byte[]... keys);

    Future<Long> sdiffstore(byte[] dst, byte[] key, byte[]... keys);

    Future<List<byte[]>> sinter(byte[] key, byte[]... keys);

    Future<Long> sinterstore(byte[] dst, byte[] key, byte[]... keys);

    Future<Boolean> sismember(byte[] key, byte[] member);

    Future<List<byte[]>> smembers(byte[] key);

    Future<Boolean> smove(byte[] src, byte[] dst, byte[] member);

    Future<byte[]> spop(byte[] key);

    Future<byte[]> srandmember(byte[] key);

    Future<List<byte[]>> srandmember(byte[] key, long count);

    Future<Long> srem(byte[] key, byte[] member, byte[]... members);

    Future<List<byte[]>> sunion(byte[] key, byte[]... keys);

    Future<Long> sunionstore(byte[] dst, byte[] key, byte[]... keys);

}
