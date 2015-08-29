package com.github.apache9.nedis;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface AsyncCloseable {

    /**
     * return a {@link Future} which will be notified after closed. This method always returns the
     * same future instance.
     */
    Future<Void> closeFuture();

    /**
     * close this instance. The return value is same with {@link #closeFuture()}.
     */
    Future<Void> close();
}
