package com.wandoulabs.nedis.protocol;

import io.netty.util.concurrent.Future;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author zhangduo
 */
public interface ListsCommands extends BlockingListsCommands {

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

    Future<byte[]> rpop(byte[] key);

    Future<byte[]> rpoplpush(byte[] src, byte[] dst);

    Future<Long> rpush(byte[] key, byte[]... values);

    Future<Long> rpushx(byte[] key, byte[] value);
}
