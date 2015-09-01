package com.github.apache9.nedis.protocol;

import java.nio.charset.StandardCharsets;

/**
 * @author Apache9
 */
public enum RedisKeyword {

    COUNT, EX, EXISTS, FLUSH, GET, GETNAME, KILL, LIMIT, LIST, LOAD, MATCH, NX, PX, REPLACE,
    RESETSTAT, REWRITE, SET, SETNAME, WITHSCORES, XX;

    public final byte[] raw;

    RedisKeyword() {
        raw = name().getBytes(StandardCharsets.UTF_8);
    }
}
