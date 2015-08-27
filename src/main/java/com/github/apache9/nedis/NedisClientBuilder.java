package com.github.apache9.nedis;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.net.InetSocketAddress;

/**
 * @author Apache9
 */
public class NedisClientBuilder {

    private final Bootstrap b = new Bootstrap();

    private NedisClientBuilder() {}

    public NedisClientBuilder group(EventLoopGroup g) {
        b.group(g);
        return this;
    }

    public NedisClientBuilder channel(Class<? extends Channel> channelClass) {
        b.channel(channelClass);
        return this;
    }

    public Future<NedisClient> connect(String host) {
        return connect(new InetSocketAddress(host, 6379));
    }

    public Future<NedisClient> connect(InetSocketAddress remoteAddr) {
        ChannelFuture f = b.handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.pipeline().addLast(new RedisRequestEncoder(), new RedisResponseDecoder(),
                        new RedisDuplexHandler());
            }

        }).connect(remoteAddr);
        final Promise<NedisClient> promise = f.channel().eventLoop().newPromise();
        f.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    promise.trySuccess(new NedisClientImpl(future.channel()));
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
