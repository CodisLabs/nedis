package com.github.apache9.nedis.protocol;

import static com.github.apache9.nedis.util.NedisUtils.toBytes;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhangduo
 */
public class ScanResult<T> {

    private static final byte[] FINISHED_CURSOR = toBytes("0");

    private final byte[] cursor;

    private final List<T> values;

    public ScanResult(byte[] cursor, List<T> values) {
        this.cursor = cursor;
        this.values = values;
    }

    public byte[] cursor() {
        return cursor;
    }

    public List<T> values() {
        return values;
    }

    public boolean more() {
        return !Arrays.equals(cursor, FINISHED_CURSOR);
    }
}
