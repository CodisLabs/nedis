package com.github.apache9.nedis;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @author Apache9
 */
public class NedisUtils {

    public static byte[] toBytes(double value) {
        return toBytes(Double.toString(value));
    }

    public static byte[] toBytes(int value) {
        return toBytes(Integer.toString(value));
    }

    public static byte[] toBytes(long value) {
        return toBytes(Long.toString(value));
    }

    public static byte[] toBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static byte[][] toParams(byte[][] tailParams, byte[]... headParams) {
        byte[][] params = Arrays.copyOf(headParams, headParams.length + tailParams.length);
        System.arraycopy(tailParams, 0, params, headParams.length, tailParams.length);
        return params;
    }
}
