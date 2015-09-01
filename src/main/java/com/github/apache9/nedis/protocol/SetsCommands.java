package com.github.apache9.nedis.protocol;

import io.netty.util.concurrent.Future;

import java.util.Set;

/**
 * @author zhangduo
 */
public interface SetsCommands {

    Future<Long> sadd(byte[] key, byte[]... members);

    Future<Long> scard(byte[] key);

    Future<Set<byte[]>> sdiff(byte[]... keys);

    Future<Long> sdiffstore(byte[] dst, byte[]... keys);

    Future<Set<byte[]>> sinter(byte[]... keys);

    Future<Long> sinterstore(byte[] dst, byte[]... keys);

    Future<Boolean> sismember(byte[] key, byte[] member);

    Future<Set<byte[]>> smembers(byte[] key);

    Future<Boolean> smove(byte[] src, byte[] dst, byte[] member);

    Future<byte[]> spop(byte[] key);

    Future<byte[]> srandmember(byte[] key);

    Future<Set<byte[]>> srandmember(byte[] key, long count);

    Future<Long> srem(byte[] key, byte[]... members);

    Future<ScanResult<byte[]>> sscan(byte[] key, ScanParams params);

    Future<Set<byte[]>> sunion(byte[]... keys);

    Future<Long> sunionstore(byte[] dst, byte[]... keys);

}
