package com.github.apache9.nedis;

import java.nio.charset.StandardCharsets;

/**
 * @author Apache9
 */
public enum RedisCommand {
    PING, ECHO, QUIT, SET, EXPIRE, EXPIREAT, GET, EXISTS, MGET, MSET, MSETNX, DEL, INCR, INCRBY, INCRBYFLOAT, DECR, DECRBY, EVAL;

    public final byte[] raw;

    RedisCommand() {
        raw = name().getBytes(StandardCharsets.US_ASCII);
    }

}
