/**
 * Copyright (c) 2015 CodisLabs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.codis.nedis.handler;

import io.codis.nedis.exception.TxnAbortException;
import io.codis.nedis.exception.TxnDiscardException;
import io.codis.nedis.protocol.TransactionsCommands;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.UnsupportedMessageTypeException;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.concurrent.Promise;

import static io.codis.nedis.protocol.RedisCommand.DISCARD;
import static io.codis.nedis.protocol.RedisCommand.EXEC;
import static io.codis.nedis.protocol.RedisCommand.MULTI;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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

    private static final Entry TXN_MARKER = new Entry(null, 0);

    private final Deque<Entry> entryQ = new ArrayDeque<>();

    private long timeoutNs;

    private ScheduledFuture<?> timeoutTask;

    private boolean inMulti;

    public RedisDuplexHandler(long timeoutNs) {
        this.timeoutNs = timeoutNs;
    }

    public long getTimeoutNs() {
        return timeoutNs;
    }

    public void setTimeoutNs(long timeoutNs) {
        this.timeoutNs = timeoutNs;
    }

    private void scheduleTimeoutTask(ChannelHandlerContext ctx) {
        if (timeoutNs > 0 && timeoutTask == null) {
            timeoutTask = ctx.executor().schedule(new TimeoutTask(ctx), timeoutNs,
                    TimeUnit.NANOSECONDS);
        }
    }

    private void writeNormal(ChannelHandlerContext ctx, RedisRequest req, ChannelPromise promise) {
        entryQ.addLast(new Entry(req.getPromise(), System.nanoTime()));
        ctx.write(req.getParams(), promise);
    }

    // How to deal with txn:
    // When MULTI, append a TXN_MARKER to the end of entryQ after the entry of MULTI command itself.
    // When EXEC or DISCARD, also append a TXN_MARKER to the end of entryQ before the entry of the
    // command itself.
    // In channelRead, if the top of entryQ is a TXN_MARKER, then decode the msg and fill the
    // entries in entryQ until reaching the next TXN_MARKER.
    private void writeTxn(ChannelHandlerContext ctx, TxnRedisRequest req, ChannelPromise promise) {
        switch (req.getCmd()) {
            case MULTI: {
                if (inMulti) {
                    req.getPromise().tryFailure(new IllegalStateException("Already in MULTI"));
                    break;
                }
                inMulti = true;
                ctx.write(RedisRequestEncoder.encode(ctx.alloc(), MULTI.raw), promise);
                entryQ.addLast(new Entry(req.getPromise(), System.nanoTime()));
                entryQ.addLast(TXN_MARKER);
                break;
            }
            case EXEC: {
                if (!inMulti) {
                    req.getPromise().tryFailure(new IllegalStateException("not in MULTI"));
                    break;
                }
                ctx.write(RedisRequestEncoder.encode(ctx.alloc(), EXEC.raw), promise);
                inMulti = false;
                entryQ.addLast(TXN_MARKER);
                entryQ.addLast(new Entry(req.getPromise(), System.nanoTime()));
                break;
            }
            case DISCARD: {
                if (!inMulti) {
                    req.getPromise().tryFailure(new IllegalStateException("not in MULTI"));
                    break;
                }
                ctx.write(RedisRequestEncoder.encode(ctx.alloc(), DISCARD.raw), promise);
                inMulti = false;
                entryQ.addLast(TXN_MARKER);
                entryQ.addLast(new Entry(req.getPromise(), System.nanoTime()));
                break;
            }
            default:
                throw new IllegalArgumentException(req.getCmd() + " is not a transactional command");
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (msg instanceof RedisRequest) {
            writeNormal(ctx, (RedisRequest) msg, promise);
        } else if (msg instanceof TxnRedisRequest) {
            writeTxn(ctx, (TxnRedisRequest) msg, promise);
        } else {
            throw new UnsupportedMessageTypeException(msg, RedisRequest.class,
                    TxnRedisRequest.class);
        }
        scheduleTimeoutTask(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg.equals(TransactionsCommands.QUEUED)) {
            // this is the reply of a command in multi, just ignore
            return;
        }
        Entry entry = entryQ.pollFirst();
        if (entry == null) {
            throw new IllegalStateException("Got response " + msg + " but no one is waiting for it");
        }
        if (entry == TXN_MARKER) {
            if (msg == RedisResponseDecoder.NULL_REPLY) {
                TxnAbortException cause = new TxnAbortException();
                while ((entry = entryQ.pollFirst()) != TXN_MARKER) {
                    entry.promise.tryFailure(cause);
                }
            } else if (msg instanceof String) {
                TxnDiscardException cause = new TxnDiscardException();
                while ((entry = entryQ.pollFirst()) != TXN_MARKER) {
                    entry.promise.tryFailure(cause);
                }
            } else {
                @SuppressWarnings("unchecked")
                Iterator<Object> iter = ((List<Object>) msg).iterator();
                while ((entry = entryQ.pollFirst()) != TXN_MARKER) {
                    entry.promise.trySuccess(iter.next());
                }
            }
            entry = entryQ.pollFirst();
        }
        entry.promise.trySuccess(msg);
    }

    private void failAll(Throwable cause) {
        for (Entry entry; (entry = entryQ.pollFirst()) != null;) {
            if (entry == TXN_MARKER) {
                continue;
            }
            entry.promise.tryFailure(cause);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (!entryQ.isEmpty()) { // only create exception if necessary
            failAll(new ClosedChannelException());
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        failAll(cause);
        ctx.close();
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
            boolean refillTxnMarker = false;
            if (entryQ.peekFirst() == TXN_MARKER) {
                entryQ.removeFirst();
                refillTxnMarker = true;
            }
            if (entryQ.isEmpty()) {
                entryQ.addFirst(TXN_MARKER);
                timeoutTask = null;
                return;
            }
            long nextDelayNs = timeoutNs - (System.nanoTime() - entryQ.peek().nanoTime);
            if (nextDelayNs <= 0) {
                exceptionCaught(ctx, ReadTimeoutException.INSTANCE);
            } else {
                timeoutTask = ctx.executor().schedule(this, nextDelayNs, TimeUnit.NANOSECONDS);
                if (refillTxnMarker) {
                    entryQ.addFirst(TXN_MARKER);
                }
            }
        }

    }
}
