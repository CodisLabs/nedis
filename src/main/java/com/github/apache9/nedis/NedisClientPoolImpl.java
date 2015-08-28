package com.github.apache9.nedis;

import static com.github.apache9.nedis.RedisCommand.AUTH;
import static com.github.apache9.nedis.RedisCommand.CLIENT;
import static com.github.apache9.nedis.RedisCommand.SELECT;
import static com.github.apache9.nedis.RedisKeyword.SETNAME;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Apache9
 */
public class NedisClientPoolImpl implements NedisClientPool {

    private final Bootstrap bootstrap;

    private final byte[] password;

    private final byte[] database;

    private final byte[] clientName;

    private final int maxPooledConns;

    private final boolean exclusive;

    private static final class NedisClientWrapper {

        public final NedisClient client;

        public NedisClientWrapper(NedisClient client) {
            this.client = client;
        }

        @Override
        public final int hashCode() {
            return System.identityHashCode(client);
        }

        @Override
        public final boolean equals(Object obj) {
            if (obj.getClass() != NedisClientWrapper.class) {
                return false;
            }
            return client == ((NedisClientWrapper) obj).client;
        }

    }

    private final LinkedHashSet<NedisClientWrapper> pool = new LinkedHashSet<>();

    private int numConns;

    private boolean closed = false;

    public NedisClientPoolImpl(Bootstrap bootstrap, final long timeoutMs, byte[] password,
            byte[] database, byte[] clientName, int maxPooledConns, boolean exclusive) {
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
    }

    private final class InitializeFutureListener implements FutureListener<Object> {

        private final Promise<NedisClient> promise;

        private final NedisClient client;

        private final State nextState;

        public InitializeFutureListener(Promise<NedisClient> promise, NedisClient client,
                State nextState) {
            this.promise = promise;
            this.client = client;
            this.nextState = nextState;
        }

        @Override
        public void operationComplete(Future<Object> future) throws Exception {
            if (future.isSuccess()) {
                Object resp = future.getNow();
                if (resp instanceof RedisResponseException) {
                    promise.tryFailure((RedisResponseException) resp);
                    client.close();
                } else {
                    initialize(promise, client, nextState);
                }
            } else {
                promise.tryFailure(future.cause());
                client.close();
            }
        }

    }

    private enum State {
        AUTH, SELECT, CLIENT_SETNAME, FINISH
    }

    private void initialize(final Promise<NedisClient> promise, final NedisClient client,
            State state) {
        switch (state) {
            case AUTH:
                if (password == null) {
                    initialize(promise, client, State.SELECT);
                } else {
                    client.execCmd(AUTH.raw, password).addListener(
                            new InitializeFutureListener(promise, client, State.SELECT));
                }
                break;
            case SELECT:
                if (database == null) {
                    initialize(promise, client, State.CLIENT_SETNAME);
                } else {
                    client.execCmd(SELECT.raw, database).addListener(
                            new InitializeFutureListener(promise, client, State.CLIENT_SETNAME));
                }
                break;
            case CLIENT_SETNAME:
                if (clientName == null) {
                    promise.trySuccess(client);
                } else {
                    client.execCmd(CLIENT.raw, SETNAME.raw, clientName).addListener(
                            new InitializeFutureListener(promise, client, State.FINISH));
                }
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
                    initialize(promise, new NedisClientImpl(future.channel()), State.AUTH);
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
                                pool.remove(new NedisClientWrapper(client));
                                numConns--;
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
                return bootstrap.group().next().<NedisClient>newPromise()
                        .setFailure(new IllegalStateException("already closed"));
            }
            if (numConns < maxPooledConns) {
                numConns++;
                return newClient().addListener(new AcquireFutureListener(exclusive));
            }
            if (!pool.isEmpty()) {
                NedisClientWrapper wrapper = pool.iterator().next();
                Promise<NedisClient> promise = wrapper.client.eventLoop().newPromise();
                promise.setSuccess(wrapper.client);
                if (exclusive) {
                    pool.remove(wrapper);
                }
                return promise;
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
        NedisClientWrapper wrapper = new NedisClientWrapper(client);
        if (pool.contains(wrapper)) {
            return;
        }
        if (pool.size() < maxPooledConns) {
            pool.add(wrapper);
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
    public void close() {
        synchronized (pool) {
            closed = true;
            for (NedisClientWrapper wrapper: pool) {
                wrapper.client.close();
            }
        }
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
