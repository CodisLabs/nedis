package com.wandoulabs.nedis.protocol;

import static com.wandoulabs.nedis.util.NedisUtils.toBytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Apache9
 */
public class ZSetOpParams {

    private final List<byte[]> keys = new ArrayList<>();

    private final List<byte[]> weights = new ArrayList<>();

    private Aggregate aggregate;

    public ZSetOpParams clear() {
        keys.clear();
        weights.clear();
        aggregate = null;
        return this;
    }

    public ZSetOpParams key(byte[] key) {
        if (!weights.isEmpty()) {
            throw new IllegalArgumentException();
        }
        keys.add(key);
        return this;
    }

    public ZSetOpParams keyWithWeight(byte[] key, double weight) {
        if (!keys.isEmpty() && weights.isEmpty()) {
            throw new IllegalArgumentException();
        }
        keys.add(key);
        weights.add(toBytes(weight));
        return this;
    }

    public ZSetOpParams aggregate(Aggregate aggregate) {
        this.aggregate = aggregate;
        return this;
    }

    public List<byte[]> keys() {
        return keys;
    }

    public List<byte[]> weights() {
        return weights;
    }

    public Aggregate aggregate() {
        return aggregate;
    }
}
