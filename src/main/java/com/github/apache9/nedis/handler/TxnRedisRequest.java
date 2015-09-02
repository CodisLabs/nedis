package com.github.apache9.nedis.handler;

import io.netty.util.concurrent.Promise;

import com.github.apache9.nedis.protocol.RedisCommand;

/**
 * @author zhangduo
 */
public class TxnRedisRequest {

    private final Promise<Object> promise;

    private final RedisCommand cmd;

    public TxnRedisRequest(Promise<Object> promise, RedisCommand cmd) {
        this.promise = promise;
        this.cmd = cmd;
    }

    public Promise<Object> getPromise() {
        return promise;
    }

    public RedisCommand getCmd() {
        return cmd;
    }

}
