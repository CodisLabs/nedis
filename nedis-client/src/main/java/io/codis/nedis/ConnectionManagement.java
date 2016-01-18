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

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface ConnectionManagement extends AsyncCloseable {
    /**
     * return previous timeout value. Possible null if the client is already closed.
     */
    Future<Long> setTimeout(long timeoutMs);

    /**
     * return the {@link EventLoop} of the underline {@link io.netty.channel.Channel}.
     */
    EventLoop eventLoop();

    /**
     * return whether the client is still open.
     */
    boolean isOpen();

    /**
     * return the client to its pool if:
     * <ol>
     * <li>The client is created by a {@link NedisClientPool}.</li>
     * <li>The pool is {@code exclusive}.</li>
     * </ol>
     */
    void release();
}
