package com.github.apache9.nedis.handler;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.concurrent.Promise;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.github.apache9.nedis.RedisRequest;

/**
 * @author Apache9
 */
public class RedisDuplexHandler extends ChannelDuplexHandler {

    private static final class Entry {

        public final Promise<Object> promise;

        public final long nanoTime;

        public Entry(Promise<Object> promise, long nanoTime) {
            this.promise = promise;
            this.nanoTime = nanoTime;
        }

    }

    private final Queue<Entry> entryQ = new ArrayDeque<>();

    private long timeoutNs;

    private ScheduledFuture<?> timeoutTask;

    public RedisDuplexHandler(long timeoutNs) {
        this.timeoutNs = timeoutNs;
    }

    public long getTimeoutNs() {
        return timeoutNs;
    }

    public void setTimeoutNs(long timeoutNs) {
        this.timeoutNs = timeoutNs;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (msg instanceof RedisRequest) {
            RedisRequest req = (RedisRequest) msg;
            entryQ.add(new Entry(req.getPromise(), System.nanoTime()));
            ctx.write(req.getParams(), promise);
            if (timeoutNs > 0 && timeoutTask == null) {
                timeoutTask = ctx.executor().schedule(new TimeoutTask(ctx), timeoutNs,
                        TimeUnit.NANOSECONDS);
            }
        } else {
            throw new UnsupportedMessageTypeException(msg, RedisRequest.class);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Entry entry = entryQ.poll();
        if (entry == null) {
            throw new IllegalStateException("Got response " + msg + " but no one is waiting for it");
        }
        entry.promise.trySuccess(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, final Throwable cause) {
        ctx.close().addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                for (Entry entry; (entry = entryQ.poll()) != null;) {
                    entry.promise.tryFailure(cause);
                }
            }
        });
    }

    private final class TimeoutTask implements Runnable {

        private final ChannelHandlerContext ctx;

        public TimeoutTask(ChannelHandlerContext ctx) {
            this.ctx = ctx;
        }

        @Override
        public void run() {
            if (entryQ.isEmpty() || timeoutNs <= 0) {
                timeoutTask = null;
                return;
            }
            long nextDelayNs = timeoutNs - (System.nanoTime() - entryQ.peek().nanoTime);
            if (nextDelayNs <= 0) {
                exceptionCaught(ctx, ReadTimeoutException.INSTANCE);
            } else {
                timeoutTask = ctx.executor().schedule(this, nextDelayNs, TimeUnit.NANOSECONDS);
            }
        }

    }
}
