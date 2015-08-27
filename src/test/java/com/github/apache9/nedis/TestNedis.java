package com.github.apache9.nedis;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Apache9
 */
public class TestNedis {

    private static int PORT = 13475;

    private static RedisServer REDIS;

    private static Channel CLIENT;

    private static EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

    @BeforeClass
    public static void setUp() throws IOException, InterruptedException {
        REDIS = new RedisServer(PORT);
        REDIS.start();
        Thread.sleep(2000);

        CLIENT = new Bootstrap().group(WORKER_GROUP).channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<Channel>() {

                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(new RedisRequestEncoder(), new RedisResponseDecoder(),
                                new RedisDuplexHandler());
                    }

                }).connect("127.0.0.1", PORT).sync().channel();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        if (CLIENT != null) {
            CLIENT.close().sync();
        }
        REDIS.stop();
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        Promise<Object> p1 = CLIENT.eventLoop().newPromise();
        CLIENT.writeAndFlush(new RedisRequest(p1, new byte[][] {
            "PING".getBytes(StandardCharsets.US_ASCII)
        }));
        Promise<Object> p2 = CLIENT.eventLoop().newPromise();
        CLIENT.writeAndFlush(new RedisRequest(p2, new byte[][] {
            "SET".getBytes(StandardCharsets.US_ASCII), "foo".getBytes(StandardCharsets.US_ASCII),
            "bar".getBytes(StandardCharsets.US_ASCII)
        }));
        System.out.println(p1.sync().get());
        System.out.println(p2.sync().get());
        Promise<Object> p3 = CLIENT.eventLoop().newPromise();
        CLIENT.writeAndFlush(new RedisRequest(p3, new byte[][] {
            "GET".getBytes(StandardCharsets.US_ASCII), "foo".getBytes(StandardCharsets.US_ASCII)
        }));
        System.out.println(new String((byte[]) p3.sync().get(), StandardCharsets.US_ASCII));
    }
}
