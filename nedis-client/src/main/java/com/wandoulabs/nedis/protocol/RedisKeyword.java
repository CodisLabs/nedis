package com.wandoulabs.nedis.protocol;

import java.nio.charset.StandardCharsets;

/**
 * @author Apache9
 */
public enum RedisKeyword {

    ALPHA, ASC, BY, COUNT, DESC, EX, EXISTS, FLUSH, GET, GETNAME, KILL, LIMIT, LIST, LOAD,
    MATCH, NX, PX, REPLACE, RESETSTAT, REWRITE, SET, SETNAME, STORE, WITHSCORES, XX;

    public final byte[] raw;

    RedisKeyword() {
        raw = name().getBytes(StandardCharsets.UTF_8);
    }
}
