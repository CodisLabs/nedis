package com.wandoulabs.nedis.protocol;

import io.netty.util.concurrent.Future;

import java.util.List;

/**
 * @author Apache9
 */
public interface KeysCommands {

    Future<Long> del(byte[]... keys);

    Future<byte[]> dump(byte[] key);

    Future<Boolean> exists(byte[] key);

    Future<Boolean> expire(byte[] key, long seconds);

    Future<Boolean> expireAt(byte[] key, long unixTimeSeconds);

    Future<List<byte[]>> keys(byte[] pattern);

    Future<Void> migrate(byte[] host, int port, byte[] key, int dstDb, long timeoutMs);

    Future<Boolean> move(byte[] key, int db);

    Future<Boolean> persist(byte[] key);

    Future<Boolean> pexpire(byte[] key, long millis);

    Future<Boolean> pexpireAt(byte[] key, long unixTimeMs);

    Future<Long> pttl(byte[] key);

    Future<byte[]> randomkey();

    Future<Void> rename(byte[] key, byte[] newKey);

    Future<Boolean> renamenx(byte[] key, byte[] newKey);

    Future<Void> restore(byte[] key, int ttlMs, byte[] serializedValue, boolean replace);

    Future<ScanResult<byte[]>> scan(ScanParams params);

    Future<List<byte[]>> sort(byte[] key);

    Future<List<byte[]>> sort(byte[] key, SortParams params);

    Future<Long> sort(byte[] key, byte[] dst);

    Future<Long> sort(byte[] key, SortParams params, byte[] dst);

    Future<Long> ttl(byte[] key);

    Future<String> type(byte[] key);
}
