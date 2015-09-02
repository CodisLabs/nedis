package com.wandoulabs.nedis.protocol;

import static com.wandoulabs.nedis.protocol.RedisKeyword.ALPHA;
import static com.wandoulabs.nedis.protocol.RedisKeyword.ASC;
import static com.wandoulabs.nedis.protocol.RedisKeyword.DESC;
import static com.wandoulabs.nedis.protocol.RedisKeyword.GET;
import static com.wandoulabs.nedis.util.NedisUtils.toBytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Apache9
 */
public class SortParams {

    private byte[] by;

    private final List<byte[]> limit = new ArrayList<>(2);

    private final List<byte[]> get = new ArrayList<>();

    private byte[] order;

    private byte[] alpha;

    public SortParams by(byte[] pattern) {
        this.by = pattern;
        return this;
    }

    public SortParams limit(long offset, long count) {
        limit.clear();
        limit.add(toBytes(offset));
        limit.add(toBytes(count));
        return this;
    }

    public SortParams get(byte[]... patterns) {
        for (byte[] pattern: patterns) {
            get.add(GET.raw);
            get.add(pattern);
        }
        return this;
    }

    public SortParams clearGet() {
        get.clear();
        return this;
    }

    public SortParams asc() {
        order = ASC.raw;
        return this;
    }

    public SortParams desc() {
        order = DESC.raw;
        return this;
    }

    public SortParams clearOrder() {
        order = null;
        return this;
    }

    public SortParams alpha() {
        alpha = ALPHA.raw;
        return this;
    }

    public SortParams clearAlpha() {
        alpha = null;
        return this;
    }

    public byte[] by() {
        return by;
    }

    public List<byte[]> limit() {
        return limit;
    }

    public List<byte[]> get() {
        return get;
    }

    public byte[] order() {
        return order;
    }

    public byte[] getAlpha() {
        return alpha;
    }

    public SortParams clear() {
        by = null;
        limit.clear();
        get.clear();
        order = null;
        alpha = null;
        return this;
    }
}
