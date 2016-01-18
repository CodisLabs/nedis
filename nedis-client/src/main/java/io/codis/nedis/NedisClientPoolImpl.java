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
package io.codis.nedis;

import io.codis.nedis.util.NedisClientHashSet;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import static io.codis.nedis.util.NedisUtils.getEventExecutor;

import java.net.SocketAddress;
import java.util.ArrayList;

/**
 * @author Apache9
 */
public class NedisClientPoolImpl implements NedisClientPool {

    private final EventLoopGroup group;

    private final Class<? extends Channel> channelClass;

    private final long timeoutMs;

    private final SocketAddress remoteAddress;

    private final byte[] password;

    private final int database;

    private final byte[] clientName;

    private final int maxPooledConns;

    private final boolean exclusive;

    private final NedisClientHashSet pool;

    private final Promise<Void> closePromise;

    private int numConns;

    private boolean closed = false;

    public NedisClientPoolImpl(EventLoopGroup group, Class<? extends Channel> channelClass,
            long timeoutMs, SocketAddress remoteAddress, byte[] password, int database,
            byte[] clientName, int maxPooledConns, boolean exclusive) {
        this.group = group;
        this.channelClass = channelClass;
        this.timeoutMs = timeoutMs;
        this.remoteAddress = remoteAddress;
        this.password = password;
        this.database = database;
        this.clientName = clientName;
        this.maxPooledConns = maxPooledConns;
        this.exclusive = exclusive;
        this.pool = new NedisClientHashSet(maxPooledConns);
        this.closePromise = group.next().newPromise();
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
        Future<NedisClientImpl> f = NedisClientBuilder.create().group(group).channel(channelClass)
                .timeoutMs(timeoutMs).belongTo(this).connect(remoteAddress);

        final Promise<NedisClient> promise = getEventExecutor(f).newPromise();
        f.addListener(new FutureListener<NedisClientImpl>() {

            @Override
            public void operationComplete(Future<NedisClientImpl> future) throws Exception {
                if (future.isSuccess()) {
                    initialize(promise, future.getNow(), State.AUTH);
                } else {
                    promise.tryFailure(future.cause());
                }
            }

        });
        return promise;
    }

    private final FutureListener<NedisClient> acquireFutureListener = new FutureListener<NedisClient>() {

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
                        if (closed) {
                            client.close();
                        } else {
                            pool.add(client);
                            if (!pendingAcquireList.isEmpty()) {
                                for (Promise<NedisClient> promise: pendingAcquireList) {
                                    promise.trySuccess(client);
                                }
                                pendingAcquireList.clear();
                                // usually we do not need this any more, so trim its size.
                                pendingAcquireList.trimToSize();
                            }
                        }
                    }
                } else {
                    numConns--;
                    if (!exclusive && numConns == 0) {
                        // notify all pending promises that we could not get a connection.
                        for (Promise<NedisClient> promise: pendingAcquireList) {
                            promise.tryFailure(future.cause());
                        }
                        pendingAcquireList.clear();
                    }
                }
            }
        }
    };

    private final ArrayList<Promise<NedisClient>> pendingAcquireList = new ArrayList<>();

    @Override
    public Future<NedisClient> acquire() {
        synchronized (pool) {
            if (closed) {
                return group.next().<NedisClient>newFailedFuture(
                        new IllegalStateException("already closed"));
            }
            if (numConns < maxPooledConns) {
                numConns++;
                return newClient().addListener(acquireFutureListener);
            }
            if (!pool.isEmpty()) {
                NedisClient client = pool.head(exclusive);
                return client.eventLoop().newSucceededFuture(client);
            }
            if (exclusive) {
                numConns++;
                return newClient().addListener(acquireFutureListener);
            } else {
                // If connection is shared, then we should not create more connections than
                // maxPooledConns. So here we add a promise to pending queue. The promise will be
                // notified when there are connections in pool.
                Promise<NedisClient> promise = group.next().newPromise();
                pendingAcquireList.add(promise);
                return promise;
            }
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
            if (numConns == 0) {
                closePromise.trySuccess(null);
                return closePromise;
            }
            toClose = pool.toArray();
        }
        for (NedisClient client: toClose) {
            client.close();
        }
        return closePromise;
    }

    @Override
    public Future<Void> closeFuture() {
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
}
