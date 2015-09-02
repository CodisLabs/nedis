package com.wandoulabs.nedis.protocol;

import java.nio.charset.StandardCharsets;

/**
 * @author Apache9
 */
public enum BitOp {

    AND, OR, XOR, NOT;

    public final byte[] raw;

    private BitOp() {
        raw = name().getBytes(StandardCharsets.UTF_8);
    }
}
