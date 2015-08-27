package com.github.apache9.nedis;

import static org.junit.Assert.*;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
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

    private static EventLoopGroup WORKER_GROUP = new NioEventLoopGroup();

    private static NedisClient CLIENT;

    @BeforeClass
    public static void setUp() throws IOException, InterruptedException {
        REDIS = new RedisServer(PORT);
        REDIS.start();
        Thread.sleep(2000);
        CLIENT = NedisClientBuilder.builder().group(WORKER_GROUP).channel(NioSocketChannel.class)
                .connect(new InetSocketAddress("127.0.0.1", PORT)).sync().getNow();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        if (CLIENT != null) {
            CLIENT.close().sync();
        }
        REDIS.stop();
    }

    private byte[] toBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private String toString(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        Future<String> pingFuture = CLIENT.ping();
        Future<Boolean> setFuture = CLIENT.set(toBytes("foo"), toBytes("bar"));
        assertEquals("PONG", pingFuture.sync().getNow());
        assertTrue(setFuture.sync().getNow());
        assertEquals("bar", toString(CLIENT.get(toBytes("foo")).sync().getNow()));
        assertEquals(null, CLIENT.get(toBytes("bar")).sync().getNow());

        Future<Long> incrFuture = CLIENT.incr(toBytes("num"));
        Future<Long> incrByFuture = CLIENT.incrBy(toBytes("num"), 2L);
        Future<Long> decrFuture = CLIENT.decr(toBytes("num"));
        Future<Long> decrByFuture = CLIENT.decrBy(toBytes("num"), 2L);
        assertEquals(1L, incrFuture.sync().getNow().longValue());
        assertEquals(3L, incrByFuture.sync().getNow().longValue());
        assertEquals(2L, decrFuture.sync().getNow().longValue());
        assertEquals(0L, decrByFuture.sync().getNow().longValue());

        CLIENT.mset(toBytes("a1"), toBytes("b1"), toBytes("a2"), toBytes("b2")).sync();

        List<byte[]> resp = CLIENT.mget(toBytes("a1"), toBytes("a2"), toBytes("a3")).sync()
                .getNow();
        assertEquals(3, resp.size());
        assertEquals("b1", toString(resp.get(0)));
        assertEquals("b2", toString(resp.get(1)));
        assertEquals(null, resp.get(2));
    }
}
