package com.wandoulabs.nedis;

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
