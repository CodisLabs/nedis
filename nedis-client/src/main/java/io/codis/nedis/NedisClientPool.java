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
package io.codis.nedis;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface NedisClientPool extends AsyncCloseable {

    /**
     * Acquire a {@link NedisClient} from this pool.
     */
    Future<NedisClient> acquire();

    /**
     * Return a {@link NedisClient} to this pool.
     * <p>
     * Usually you should not call this method directly, call {@link NedisClient#release()} instead.
     */
    void release(NedisClient client);

    /**
     * Whether we should remove the {@link NedisClient} from pool when acquiring.
     * <p>
     * If you are doing time-consuming operations(such as {@code BLPOP}) then you should set this to
     * {@code true}
     */
    boolean exclusive();

    /**
     * Total number of alive {@link NedisClient}s.
     * <p>
     * This could be larger than {@link #numPooledConns()} if {@link #exclusive()} is {@code true}.
     */
    int numConns();

    /**
     * Number of {@link NedisClient}s in pool.
     */
    int numPooledConns();
}
