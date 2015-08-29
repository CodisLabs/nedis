package com.github.apache9.nedis;

import static com.github.apache9.nedis.NedisUtils.bytesToString;
import static com.github.apache9.nedis.NedisUtils.toBytes;
import static com.github.apache9.nedis.protocol.RedisCommand.GET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.concurrent.Future;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Apache9
 */
public class TestNedis {

    private static int PORT = 13475;

    private static RedisServer REDIS;

    private NedisClientPool pool;

    @BeforeClass
    public static void setUp() throws IOException, InterruptedException {
        REDIS = new RedisServer(PORT);
        REDIS.start();
        Thread.sleep(2000);
    }

    @AfterClass
    public static void tearDownAfterClass() throws InterruptedException {
        REDIS.stop();
    }

    private static void cleanRedis() throws InterruptedException {
        NedisClientPool p = NedisClientPoolBuilder.builder()
                .remoteAddress(new InetSocketAddress("127.0.0.1", PORT)).build();
        List<byte[]> allKeys = p.acquire().sync().getNow().keys(toBytes("*")).sync().getNow();
        if (!allKeys.isEmpty()) {
            p.acquire().sync().getNow().del(allKeys.toArray(new byte[0][])).sync();
        }
        p.close();
    }

    @After
    public void tearDown() throws InterruptedException {
        if (pool != null) {
            pool.close();
        }
        cleanRedis();
    }

    @Test
    public void test() throws InterruptedException, ExecutionException {
        pool = NedisClientPoolBuilder.builder()
                .remoteAddress(new InetSocketAddress("127.0.0.1", PORT)).build();
        NedisClient client = NedisUtils.newPooledClient(pool);
        System.out.println(client.toString());
        Future<String> pingFuture = client.ping();
        for (int i = 0; i < 1000; i++) {
            Future<Boolean> setFuture = client.set(toBytes("foo"), toBytes("bar" + i));
            assertTrue(setFuture.sync().getNow());
            assertEquals("bar" + i, bytesToString(client.get(toBytes("foo")).sync().getNow()));
        }
        assertEquals("PONG", pingFuture.sync().getNow());
        assertEquals(null, client.get(toBytes("bar")).sync().getNow());

        NedisClient pipelineClient = pool.acquire().sync().getNow();
        Future<Long> incrFuture = pipelineClient.incr(toBytes("num"));
        Future<Long> incrByFuture = pipelineClient.incrBy(toBytes("num"), 2L);
        Future<Long> decrFuture = pipelineClient.decr(toBytes("num"));
        Future<Long> decrByFuture = pipelineClient.decrBy(toBytes("num"), 2L);
        assertEquals(1L, incrFuture.sync().getNow().longValue());
        assertEquals(3L, incrByFuture.sync().getNow().longValue());
        assertEquals(2L, decrFuture.sync().getNow().longValue());
        assertEquals(0L, decrByFuture.sync().getNow().longValue());
        pipelineClient.release();

        client.mset(toBytes("a1"), toBytes("b1"), toBytes("a2"), toBytes("b2")).sync();

        List<byte[]> resp = client.mget(toBytes("a1"), toBytes("a2"), toBytes("a3")).sync()
                .getNow();
        assertEquals(3, resp.size());
        assertEquals("b1", bytesToString(resp.get(0)));
        assertEquals("b2", bytesToString(resp.get(1)));
        assertEquals(null, resp.get(2));

        assertEquals(pool.numConns(), pool.numPooledConns());
        int numConns = pool.numConns();
        Throwable error = client.execCmd(GET.raw).await().cause();
        error.printStackTrace();
        assertTrue(error instanceof RedisResponseException);

        // this error does not cause a connection closing.
        assertEquals(numConns, pool.numConns());
        assertEquals(numConns, pool.numPooledConns());

        client.close().sync();

        assertEquals(0, pool.numPooledConns());
        assertEquals(0, pool.numConns());
    }

    @Test
    public void testTimeout() throws InterruptedException {
        pool = NedisClientPoolBuilder.builder()
                .remoteAddress(new InetSocketAddress("127.0.0.1", PORT)).build();
        NedisClient client = pool.acquire().sync().getNow();
        assertEquals(1, pool.numPooledConns());
        assertEquals(1, pool.numConns());
        assertEquals(0L, client.setTimeout(100).sync().getNow().longValue());
        Future<?> future = client.blpop(1, toBytes("foo")).await();
        assertFalse(future.isSuccess());
        assertTrue(future.cause() instanceof ReadTimeoutException);
        Thread.sleep(1000);
        assertEquals(0, pool.numPooledConns());
        assertEquals(0, pool.numConns());
    }

    @Test
    public void testBlockingCommands() throws InterruptedException {
        pool = NedisClientPoolBuilder.builder()
                .remoteAddress(new InetSocketAddress("127.0.0.1", PORT)).timeoutMs(100)
                .exclusive(true).build();
        NedisClient client = NedisUtils.newPooledClient(pool);
        Future<List<byte[]>> brpopFuture = client.brpop(100, toBytes("foo"));
        Thread.sleep(1000);
        assertFalse(brpopFuture.isDone());
        client.lpush(toBytes("foo"), toBytes("bar"));
        List<byte[]> brpopResp = brpopFuture.sync().getNow();
        assertEquals(2, brpopResp.size());
        assertEquals("foo", bytesToString(brpopResp.get(0)));
        assertEquals("bar", bytesToString(brpopResp.get(1)));

        Future<List<byte[]>> blpopFuture = client.blpop(100, toBytes("a1"));
        Future<byte[]> brpoplpushFuture = client.brpoplpush(toBytes("a2"), toBytes("a1"), 100);
        Thread.sleep(1000);
        assertFalse(blpopFuture.isDone());
        assertFalse(brpoplpushFuture.isDone());
        client.lpush(toBytes("a2"), toBytes("b"));

        List<byte[]> blpopResp = blpopFuture.sync().getNow();
        assertEquals(2, blpopResp.size());
        assertEquals("a1", bytesToString(blpopResp.get(0)));
        assertEquals("b", bytesToString(blpopResp.get(1)));

        assertTrue(brpoplpushFuture.isDone());
        assertEquals("b", bytesToString(brpoplpushFuture.getNow()));

    }
}
