package com.wandoulabs.nedis.protocol;

import java.nio.charset.StandardCharsets;


/**
 * @author Apache9
 */
public enum Aggregate {
    SUM, MIN, MAX;

    public final byte[] raw;

    Aggregate() {
        raw = name().getBytes(StandardCharsets.UTF_8);
    }
}
