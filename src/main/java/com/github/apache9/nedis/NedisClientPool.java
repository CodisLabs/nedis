package com.github.apache9.nedis;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface NedisClientPool extends AsyncCloseable {

    Future<NedisClient> acquire();

    void release(NedisClient client);

    boolean exclusive();

    int numConns();

    int numPooledConns();
}
