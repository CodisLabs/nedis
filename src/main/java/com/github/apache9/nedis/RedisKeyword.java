package com.github.apache9.nedis;

import java.nio.charset.StandardCharsets;

/**
 * @author Apache9
 */
public enum RedisKeyword {

    EX, NX, PX, SETNAME, XX;

    public final byte[] raw;

    RedisKeyword() {
        raw = name().getBytes(StandardCharsets.US_ASCII);
    }

}
