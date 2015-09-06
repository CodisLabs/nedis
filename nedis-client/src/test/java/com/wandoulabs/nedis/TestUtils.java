/**
 * Copyright (c) 2015 Wandoujia Inc.
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
package com.wandoulabs.nedis;

import static com.wandoulabs.nedis.util.NedisUtils.toBytes;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

/**
 * @author Apache9
 */
public class TestUtils {

    public static final double ERROR = 1E-4;

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
}
