package com.github.apache9.nedis;

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

    EventLoop eventLoop();

    boolean isOpen();

    void release();
}
