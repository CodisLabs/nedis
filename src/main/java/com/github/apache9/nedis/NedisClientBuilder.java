package com.github.apache9.nedis;

import static com.github.apache9.nedis.NedisUtils.toBytes;
import static com.github.apache9.nedis.RedisCommand.AUTH;
import static com.github.apache9.nedis.RedisCommand.CLIENT;
import static com.github.apache9.nedis.RedisCommand.SELECT;
import static com.github.apache9.nedis.RedisKeyword.SETNAME;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;

/**
 * @author Apache9
 */
public class NedisClientBuilder {

    private EventLoopGroup group;

    private Class<? extends Channel> channelClass;

    private byte[] password;

    private byte[] database;

    private byte[] clientName;

    private NedisClientBuilder() {}

    public NedisClientBuilder group(EventLoopGroup group) {
        this.group = group;
        return this;
    }

    public NedisClientBuilder channel(Class<? extends Channel> channelClass) {
        this.channelClass = channelClass;
        return this;
    }

    public NedisClientBuilder password(String password) {
        this.password = toBytes(password);
        return this;
    }

    public NedisClientBuilder database(int database) {
        this.database = toBytes(database);
        return this;
    }

    public NedisClientBuilder clientName(String clientName) {
        this.clientName = toBytes(clientName);
        return this;
    }

    public Future<NedisClient> connect(String host) {
        return connect(host, 6379);
    }

    public Future<NedisClient> connect(String host, int port) {
        return connect(new InetSocketAddress(host, port));
    }

    private enum State {
        AUTH, SELECT, CLIENT_SETNAME, FINISH
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
                } else {
                    initialize(promise, client, nextState);
                }
            } else {
                promise.tryFailure(future.cause());
            }
        }

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

    public Future<NedisClient> connect(InetSocketAddress remoteAddr) {
        ChannelFuture f = new Bootstrap().group(group).channel(channelClass)
                .handler(new ChannelInitializer<Channel>() {

                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(new RedisRequestEncoder(),
                                new RedisResponseDecoder(), new RedisDuplexHandler());
                    }

                }).connect(remoteAddr);
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

    public static NedisClientBuilder builder() {
        return new NedisClientBuilder();
    }
}
