/**
 * Copyright (c) 2015 CodisLabs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.codis.nedis.protocol;

import static io.codis.nedis.util.NedisUtils.toBytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Apache9
 * @see http://redis.io/commands/zinterstore
 * @see http://redis.io/commands/zunionstore
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
