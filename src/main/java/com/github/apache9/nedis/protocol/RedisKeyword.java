package com.github.apache9.nedis.protocol;

import java.nio.charset.StandardCharsets;

/**
 * @author Apache9
 */
public enum RedisKeyword {

    COUNT, EX, GETNAME, MATCH, NX, PX, REPLACE, SETNAME, XX, KILL, LIST, GET, RESETSTAT, REWRITE, SET;

    public final byte[] raw;

    RedisKeyword() {
        raw = name().getBytes(StandardCharsets.UTF_8);
    }
}
