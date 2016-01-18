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

import io.codis.nedis.handler.RedisDuplexHandler;
import io.codis.nedis.handler.RedisResponseDecoder;
import io.codis.nedis.util.AbstractNedisBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import static io.codis.nedis.util.NedisUtils.DEFAULT_REDIS_PORT;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author Apache9
 */
public class NedisClientBuilder extends AbstractNedisBuilder {

    private NedisClientPool pool;

    @Override
    public NedisClientBuilder group(EventLoopGroup group) {
        super.group(group);
        return this;
    }

    @Override
    public NedisClientBuilder channel(Class<? extends Channel> channelClass) {
        super.channel(channelClass);
        return this;
    }

    @Override
    public NedisClientBuilder timeoutMs(long timeoutMs) {
        super.timeoutMs(timeoutMs);
        return this;
    }

    public NedisClientBuilder belongTo(NedisClientPool pool) {
        this.pool = pool;
        return this;
    }

    public Future<NedisClientImpl> connect(String host) {
        return connect(host, DEFAULT_REDIS_PORT);
    }

    public Future<NedisClientImpl> connect(String inetHost, int inetPort) {
        return connect(new InetSocketAddress(inetHost, inetPort));
    }

    public Future<NedisClientImpl> connect(SocketAddress remoteAddress) {
        validateGroupConfig();
        Bootstrap b = new Bootstrap().group(group).channel(channelClass)
                .handler(new ChannelInitializer<Channel>() {

                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(new RedisResponseDecoder(),
                                new RedisDuplexHandler(TimeUnit.MILLISECONDS.toNanos(timeoutMs)));
                    }

                });
        if (timeoutMs > 0) {
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                    (int) Math.min(Integer.MAX_VALUE, timeoutMs));
        }
        ChannelFuture f = b.connect(remoteAddress);
        final Promise<NedisClientImpl> promise = f.channel().eventLoop().newPromise();
        f.addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    promise.trySuccess(new NedisClientImpl(future.channel(), pool));
                } else {
                    promise.tryFailure(future.cause());
                }
            }
        });
        return promise;
    }

    private NedisClientBuilder() {}

    public static NedisClientBuilder create() {
        return new NedisClientBuilder();
    }
}
