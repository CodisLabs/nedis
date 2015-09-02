package com.wandoulabs.nedis.protocol;

/**
 * @author Apache9
 */
public class HashEntry {

    private final byte[] field;

    private final byte[] value;

    public HashEntry(byte[] field, byte[] value) {
        this.field = field;
        this.value = value;
    }

    public byte[] field() {
        return field;
    }

    public byte[] value() {
        return value;
    }

}
