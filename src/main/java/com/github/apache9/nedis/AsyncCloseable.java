/**
 * 
 */
package com.github.apache9.nedis;

import io.netty.util.concurrent.Future;

/**
 * @author zhangduo
 */
public interface AsyncCloseable {

    Future<Void> closeFuture();

    Future<Void> close();
}
