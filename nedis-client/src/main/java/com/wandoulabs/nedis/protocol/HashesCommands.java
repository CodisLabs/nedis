package com.wandoulabs.nedis.protocol;

import io.netty.util.concurrent.Future;

import java.util.List;
import java.util.Map;

/**
 * @author Apache9
 */
public interface HashesCommands {

    Future<Long> hdel(byte[] key, byte[]... fields);

    Future<Boolean> hexists(byte[] key, byte[] field);

    Future<byte[]> hget(byte[] key, byte[] field);

    Future<Map<byte[], byte[]>> hgetAll(byte[] key);

    Future<Long> hincrby(byte[] key, byte[] field, long delta);

    Future<Double> hincrbyfloat(byte[] key, byte[] field, double delta);

    Future<List<byte[]>> hkeys(byte[] key);

    Future<Long> hlen(byte[] key);

    Future<List<byte[]>> hmget(byte[] key, byte[]... fields);

    Future<Void> hmset(byte[] key, Map<byte[], byte[]> field2Value);

    Future<ScanResult<HashEntry>> hscan(byte[] key, ScanParams params);

    Future<Boolean> hset(byte[] key, byte[] field, byte[] value);

    Future<Boolean> hsetnx(byte[] key, byte[] field, byte[] value);

    Future<List<byte[]>> hvals(byte[] key);
}
