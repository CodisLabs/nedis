package com.github.apache9.nedis;

import java.io.Closeable;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface NedisClientPool extends Closeable {

    Future<NedisClient> acquire();

    void release(NedisClient client);

    int numConns();

    int numPooledConns();

    @Override
    void close();
}
