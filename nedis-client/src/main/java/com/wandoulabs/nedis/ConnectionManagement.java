package com.wandoulabs.nedis;

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
