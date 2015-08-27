package com.github.apache9.nedis;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.util.concurrent.Promise;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * @author Apache9
 */
public class RedisDuplexHandler extends ChannelDuplexHandler {

    private final Queue<Promise<Object>> promiseQ = new ArrayDeque<>();

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (msg instanceof RedisRequest) {
            RedisRequest req = (RedisRequest) msg;
            promiseQ.add(req.getPromise());
            ctx.write(req.getParams(), promise);
        } else {
            throw new UnsupportedMessageTypeException(msg, RedisRequest.class);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Promise<Object> p = promiseQ.poll();
        if (p == null) {
            throw new IllegalStateException("Got response " + msg + " but no one is waiting for it");
        }
        p.trySuccess(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        ctx.close().addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                for (Promise<Object> p; (p = promiseQ.poll()) != null;) {
                    p.tryFailure(cause);
                }
            }
        });
    }

}
