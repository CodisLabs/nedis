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

import static io.codis.nedis.protocol.RedisKeyword.ALPHA;
import static io.codis.nedis.protocol.RedisKeyword.ASC;
import static io.codis.nedis.protocol.RedisKeyword.DESC;
import static io.codis.nedis.protocol.RedisKeyword.GET;
import static io.codis.nedis.util.NedisUtils.toBytes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Apache9
 * @see http://redis.io/commands/sort
 */
public class SortParams {

    private byte[] by;

    private final List<byte[]> limit = new ArrayList<>(2);

    private final List<byte[]> get = new ArrayList<>();

    private byte[] order;

    private byte[] alpha;

    public SortParams by(byte[] pattern) {
        this.by = pattern;
        return this;
    }

    public SortParams limit(long offset, long count) {
        limit.clear();
        limit.add(toBytes(offset));
        limit.add(toBytes(count));
        return this;
    }

    public SortParams get(byte[]... patterns) {
        for (byte[] pattern: patterns) {
            get.add(GET.raw);
            get.add(pattern);
        }
        return this;
    }

    public SortParams clearGet() {
        get.clear();
        return this;
    }

    public SortParams asc() {
        order = ASC.raw;
        return this;
    }

    public SortParams desc() {
        order = DESC.raw;
        return this;
    }

    public SortParams clearOrder() {
        order = null;
        return this;
    }

    public SortParams alpha() {
        alpha = ALPHA.raw;
        return this;
    }

    public SortParams clearAlpha() {
        alpha = null;
        return this;
    }

    public byte[] by() {
        return by;
    }

    public List<byte[]> limit() {
        return limit;
    }

    public List<byte[]> get() {
        return get;
    }

    public byte[] order() {
        return order;
    }

    public byte[] getAlpha() {
        return alpha;
    }

    public SortParams clear() {
        by = null;
        limit.clear();
        get.clear();
        order = null;
        alpha = null;
        return this;
    }
}
