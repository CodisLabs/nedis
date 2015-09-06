/**
 * Copyright (c) 2015 Wandoujia Inc.
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
package com.wandoulabs.nedis;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.util.concurrent.TimeUnit;

import com.wandoulabs.nedis.handler.RedisDuplexHandler;
import com.wandoulabs.nedis.handler.RedisRequestEncoder;
import com.wandoulabs.nedis.handler.RedisResponseDecoder;
import com.wandoulabs.nedis.util.NedisClientHashSet;

/**
 * @author Apache9
 */
public class NedisClientPoolImpl implements NedisClientPool {

    private final Bootstrap bootstrap;

    private final byte[] password;

    private final int database;

    private final byte[] clientName;

    private final int maxPooledConns;

    private final boolean exclusive;

    private final NedisClientHashSet pool;

    private final Promise<Void> closePromise;

    private int numConns;

    private boolean closed = false;

    public NedisClientPoolImpl(Bootstrap bootstrap, final long timeoutMs, byte[] password,
            int database, byte[] clientName, int maxPooledConns, boolean exclusive) {
        this.bootstrap = bootstrap.handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(new RedisRequestEncoder(), new RedisResponseDecoder(),
                        new RedisDuplexHandler(TimeUnit.MILLISECONDS.toNanos(timeoutMs)));
            }

        });
        if (timeoutMs > 0) {
            this.bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                    (int) Math.min(Integer.MAX_VALUE, timeoutMs));
        }
        this.password = password;
        this.database = database;
        this.clientName = clientName;
        this.maxPooledConns = maxPooledConns;
        this.exclusive = exclusive;
        this.pool = new NedisClientHashSet(maxPooledConns);
        this.closePromise = bootstrap.group().next().newPromise();
    }

    private final class InitializeFutureListener implements FutureListener<Void> {

        private final Promise<NedisClient> promise;

        private final NedisClientImpl client;

        private final State nextState;

        public InitializeFutureListener(Promise<NedisClient> promise, NedisClientImpl client,
                State nextState) {
            this.promise = promise;
            this.client = client;
            this.nextState = nextState;
        }

        @Override
        public void operationComplete(Future<Void> future) throws Exception {
            if (future.isSuccess()) {
                initialize(promise, client, nextState);
            } else {
                promise.tryFailure(future.cause());
                client.close();
            }
        }

    }

    private enum State {
        AUTH, SELECT, CLIENT_SETNAME, FINISH
    }

    private void initialize(final Promise<NedisClient> promise, final NedisClientImpl client,
            State state) {
        switch (state) {
            case AUTH:
                if (password == null) {
                    initialize(promise, client, State.SELECT);
                } else {
                    client.auth0(password).addListener(
                            new InitializeFutureListener(promise, client, State.SELECT));
                }
                break;
            case SELECT:
                if (database == 0) {
                    initialize(promise, client, State.CLIENT_SETNAME);
                } else {
                    client.select0(database).addListener(
                            new InitializeFutureListener(promise, client, State.CLIENT_SETNAME));
                }
                break;
            case CLIENT_SETNAME:
                if (clientName == null) {
                    promise.trySuccess(client);
                } else {
                    client.clientSetname0(clientName).addListener(
                            new InitializeFutureListener(promise, client, State.FINISH));
                }
                break;
            case FINISH:
                promise.trySuccess(client);
                break;
        }
    }

    private Future<NedisClient> newClient() {
        ChannelFuture f = bootstrap.connect();
        final Promise<NedisClient> promise = f.channel().eventLoop().newPromise();
        f.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    initialize(promise, new NedisClientImpl(future.channel(),
                            NedisClientPoolImpl.this), State.AUTH);
                } else {
                    promise.tryFailure(future.cause());
                }
            }
        });
        return promise;
    }

    private final class AcquireFutureListener implements FutureListener<NedisClient> {

        private final boolean exclusive;

        public AcquireFutureListener(boolean exclusive) {
            this.exclusive = exclusive;
        }

        @Override
        public void operationComplete(Future<NedisClient> future) throws Exception {
            synchronized (pool) {
                if (future.isSuccess()) {
                    final NedisClient client = future.getNow();
                    client.closeFuture().addListener(new FutureListener<Void>() {

                        @Override
                        public void operationComplete(Future<Void> future) throws Exception {
                            synchronized (pool) {
                                pool.remove(client);
                                numConns--;
                                if (closed && numConns == 0) {
                                    closePromise.trySuccess(null);
                                }
                            }
                        }

                    });
                    if (!exclusive) {
                        tryPooling(future.getNow());
                    }
                } else {
                    numConns--;
                }
            }
        }
    }

    @Override
    public Future<NedisClient> acquire() {
        synchronized (pool) {
            if (closed) {
                return bootstrap.group().next()
                        .<NedisClient>newFailedFuture(new IllegalStateException("already closed"));
            }
            if (numConns < maxPooledConns) {
                numConns++;
                return newClient().addListener(new AcquireFutureListener(exclusive));
            }
            if (!pool.isEmpty()) {
                NedisClient client = pool.head(exclusive);
                return client.eventLoop().newSucceededFuture(client);
            }
            numConns++;
            return newClient().addListener(new AcquireFutureListener(exclusive));
        }
    }

    private void tryPooling(NedisClient client) {
        if (closed) {
            client.close();
            return;
        }
        if (pool.contains(client)) {
            return;
        }
        if (pool.size() < maxPooledConns) {
            pool.add(client);
        } else {
            client.close();
        }
    }

    @Override
    public void release(NedisClient client) {
        synchronized (pool) {
            if (client.isOpen()) {
                tryPooling(client);
            }
        }
    }

    @Override
    public Future<Void> close() {
        NedisClient[] toClose;
        synchronized (pool) {
            if (closed) {
                return closePromise;
            }
            closed = true;
            toClose = pool.toArray();
        }
        for (NedisClient client: toClose) {
            client.close();
        }
        return closePromise;
    }

    @Override
    public boolean exclusive() {
        return exclusive;
    }

    @Override
    public int numConns() {
        synchronized (pool) {
            return numConns;
        }
    }

    @Override
    public int numPooledConns() {
        synchronized (pool) {
            return pool.size();
        }
    }

    @Override
    public Future<Void> closeFuture() {
        return closePromise;
    }

}
