package com.github.apache9.nedis.protocol;

import static com.github.apache9.nedis.util.NedisUtils.toBytes;

/**
 * @author Apache9
 */
public class ScanParams {

    private static final byte[] START_CURSOR = toBytes("0");

    private byte[] cursor = START_CURSOR;

    private byte[] pattern;

    private long count;

    public byte[] cursor() {
        return cursor;
    }

    public ScanParams cursor(byte[] cursor) {
        this.cursor = cursor != null ? cursor : START_CURSOR;
        return this;
    }

    public byte[] match() {
        return pattern;
    }

    public ScanParams match(byte[] pattern) {
        this.pattern = pattern;
        return this;
    }

    public long count() {
        return count;
    }

    public ScanParams count(long count) {
        this.count = count;
        return this;
    }
}
