package com.wandoulabs.nedis;

import static com.wandoulabs.nedis.util.NedisUtils.toBytes;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.wandoulabs.nedis.NedisClient;
import com.wandoulabs.nedis.NedisClientPool;
import com.wandoulabs.nedis.NedisClientPoolBuilder;

/**
 * @author Apache9
 */
public class TestUtils {

    public static int probeFreePort() throws IOException {
        try (ServerSocket ss = new ServerSocket(0)) {
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        }
    }

    public static void cleanRedis(int port) {
        NedisClientPool pool = NedisClientPoolBuilder.builder().remoteAddress("127.0.0.1", port)
                .build();
        NedisClient client = pool.acquire().syncUninterruptibly().getNow();
        List<byte[]> keys = client.keys(toBytes("*")).syncUninterruptibly().getNow();
        if (!keys.isEmpty()) {
            client.del(keys.toArray(new byte[0][])).syncUninterruptibly();
        }
        pool.close().syncUninterruptibly();
    }

    public static void waitUntilRedisUp(int port) throws InterruptedException {
        NedisClientPool pool = NedisClientPoolBuilder.builder().remoteAddress("127.0.0.1", port)
                .maxPooledConns(1).timeoutMs(100).build();
        for (;;) {
            try {
                if ("PONG".equals(pool.acquire().syncUninterruptibly().getNow().ping()
                        .syncUninterruptibly().getNow())) {
                    break;
                }
            } catch (Exception e) {
                Thread.sleep(200);
            }
        }
        pool.close().syncUninterruptibly();
    }

    public static <T> void assertSetEquals(Set<T> expected, Set<T> actual) {
        Set<T> onlyInExpected = Sets.difference(expected, actual);
        Set<T> onlyInActual = Sets.difference(actual, expected);
        if (!onlyInExpected.isEmpty() || !onlyInActual.isEmpty()) {
            fail("Only in expected: " + onlyInExpected + ", only in actual: " + onlyInActual);
        }
    }

    public static <K, V> void assertMapEquals(Map<K, V> expected, Map<K, V> actual) {
        MapDifference<K, V> diff = Maps.difference(expected, actual);
        assertTrue(diff.toString(), diff.areEqual());
    }
}
