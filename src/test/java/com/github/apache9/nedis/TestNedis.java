package com.github.apache9.nedis;

import static com.github.apache9.nedis.NedisUtils.toBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.concurrent.Future;

import java.io.IOException;
import java.net.InetSocketAddress;
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

    private static NedisClientPool POOL;

    @BeforeClass
    public static void setUp() throws IOException, InterruptedException {
        REDIS = new RedisServer(PORT);
        REDIS.start();
        Thread.sleep(2000);
        POOL = NedisClientPoolBuilder.builder()
                .remoteAddress(new InetSocketAddress("127.0.0.1", PORT)).build();
    }

    @AfterClass
    public static void tearDown() throws InterruptedException {
        if (POOL != null) {
            POOL.close();
        }
        REDIS.stop();
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        NedisClient client = POOL.acquire().sync().getNow();
        Future<String> pingFuture = client.ping();
        Future<Boolean> setFuture = client.set(toBytes("foo"), toBytes("bar"));
        assertEquals("PONG", pingFuture.sync().getNow());
        assertTrue(setFuture.sync().getNow());
        assertEquals("bar", NedisUtils.toString(client.get(toBytes("foo")).sync().getNow()));
        assertEquals(null, client.get(toBytes("bar")).sync().getNow());

        Future<Long> incrFuture = client.incr(toBytes("num"));
        Future<Long> incrByFuture = client.incrBy(toBytes("num"), 2L);
        Future<Long> decrFuture = client.decr(toBytes("num"));
        Future<Long> decrByFuture = client.decrBy(toBytes("num"), 2L);
        assertEquals(1L, incrFuture.sync().getNow().longValue());
        assertEquals(3L, incrByFuture.sync().getNow().longValue());
        assertEquals(2L, decrFuture.sync().getNow().longValue());
        assertEquals(0L, decrByFuture.sync().getNow().longValue());

        client.mset(toBytes("a1"), toBytes("b1"), toBytes("a2"), toBytes("b2")).sync();

        List<byte[]> resp = client.mget(toBytes("a1"), toBytes("a2"), toBytes("a3")).sync()
                .getNow();
        assertEquals(3, resp.size());
        assertEquals("b1", NedisUtils.toString(resp.get(0)));
        assertEquals("b2", NedisUtils.toString(resp.get(1)));
        assertEquals(null, resp.get(2));

        assertEquals(1, POOL.numPooledConns());
        assertEquals(1, POOL.numConns());
    }

    @Test
    public void testTimeout() throws InterruptedException {
        NedisClient client = POOL.acquire().sync().getNow();
        assertEquals(1, POOL.numPooledConns());
        assertEquals(1, POOL.numConns());
        assertEquals(0L, client.setTimeout(100).sync().getNow().longValue());
        Future<?> future = client.blpop(1, toBytes("foo")).await();
        assertFalse(future.isSuccess());
        assertTrue(future.cause() instanceof ReadTimeoutException);
        Thread.sleep(1000);
        assertEquals(0, POOL.numPooledConns());
        assertEquals(0, POOL.numConns());
    }
}
