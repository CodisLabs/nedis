package com.wandoulabs.nedis.handler;

import io.netty.util.concurrent.Promise;

/**
 * @author Apache9
 */
public class RedisRequest {

    private final Promise<Object> promise;

    private final byte[][] params;

    public RedisRequest(Promise<Object> promise, byte[][] params) {
        this.promise = promise;
        this.params = params;
    }

    public Promise<Object> getPromise() {
        return promise;
    }

    public byte[][] getParams() {
        return params;
    }

}
