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

import static io.codis.nedis.util.NedisUtils.toBytes;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import io.codis.nedis.NedisClient;
import io.codis.nedis.NedisClientBuilder;

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

    public static void cleanRedis(int port) throws IOException, InterruptedException {
        NedisClient client = NedisClientBuilder.create().timeoutMs(100).connect("127.0.0.1", port)
                .sync().getNow();
        try {
            List<byte[]> keys = client.keys(toBytes("*")).sync().getNow();
            if (!keys.isEmpty()) {
                client.del(keys.toArray(new byte[0][])).sync();
            }
        } finally {
            client.close().sync();
        }

    }

    public static void waitUntilRedisUp(int port) throws InterruptedException {
        for (;;) {
            NedisClient client = null;
            try {
                client = NedisClientBuilder.create().timeoutMs(100).connect("127.0.0.1", port)
                        .sync().getNow();
                if ("PONG".equals(client.ping().sync().getNow())) {
                    break;
                }
            } catch (Exception e) {
                Thread.sleep(200);
            } finally {
                if (client != null) {
                    client.close().sync();
                }
            }
        }
    }
}
