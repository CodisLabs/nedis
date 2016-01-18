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
package io.codis.nedis.codis;

import static io.codis.nedis.TestUtils.probeFreePort;
import static io.codis.nedis.TestUtils.waitUntilRedisUp;
import static io.codis.nedis.util.NedisUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.codis.nedis.NedisClient;
import io.codis.nedis.NedisClientPoolBuilder;
import io.codis.nedis.RedisServer;
import io.codis.nedis.codis.RoundRobinNedisClientPool;
import io.codis.nedis.util.NedisUtils;

/**
 * @author Apache9
 */
public class TestRoundRobinJedisPool {

    private ObjectMapper mapper = new ObjectMapper();

    private int zkPort;

    private File testDir = new File(getClass().getName());

    private ZooKeeperServerWapper zkServer;

    private int redisPort1;

    private RedisServer redis1;

    private int redisPort2;

    private RedisServer redis2;

    private NedisClient client1;

    private NedisClient client2;

    private RoundRobinNedisClientPool clientPool;

    private NedisClient client;

    private String zkPath = "/" + getClass().getName();

    private void deleteDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }
        Files.walkFileTree(directory.toPath(), new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

        });
    }

    private void addNode(String name, int port, String state) throws IOException,
            InterruptedException, KeeperException {
        ZooKeeper zk = new ZooKeeper("localhost:" + zkPort, 5000, null);
        try {
            if (zk.exists(zkPath, null) == null) {
                zk.create(zkPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            ObjectNode node = mapper.createObjectNode();
            node.put("addr", "127.0.0.1:" + port);
            node.put("state", state);
            zk.create(zkPath + "/" + name, mapper.writer().writeValueAsBytes(node),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } finally {
            zk.close();
        }
    }

    private void removeNode(String name) throws InterruptedException, KeeperException, IOException {
        ZooKeeper zk = new ZooKeeper("localhost:" + zkPort, 5000, null);
        try {
            zk.delete(zkPath + "/" + name, -1);
        } finally {
            zk.close();
        }
    }

    private void waitUntilZkUp(int port) throws InterruptedException {
        for (;;) {
            ZooKeeper zk = null;
            try {
                zk = new ZooKeeper("127.0.0.1:" + port, 5000, null);
                zk.getChildren("/", null);
                return;
            } catch (Exception e) {
                Thread.sleep(200);
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        deleteDirectory(testDir);
        testDir.mkdirs();

        zkPort = probeFreePort();
        zkServer = new ZooKeeperServerWapper(zkPort, testDir);
        zkServer.start();
        waitUntilZkUp(zkPort);

        redisPort1 = probeFreePort();
        redis1 = new RedisServer(redisPort1);
        redis1.start();
        waitUntilRedisUp(redisPort1);

        redisPort2 = probeFreePort();
        redis2 = new RedisServer(redisPort2);
        redis2.start();
        waitUntilRedisUp(redisPort2);

        NedisClientPoolBuilder poolBuilder = NedisClientPoolBuilder.create();

        client1 = NedisUtils.newPooledClient(poolBuilder.remoteAddress("localhost", redisPort1)
                .build());
        client2 = NedisUtils.newPooledClient(poolBuilder.remoteAddress("localhost", redisPort2)
                .build());

        addNode("node1", redisPort1, "online");
        clientPool = RoundRobinNedisClientPool.builder().poolBuilder(poolBuilder)
                .curatorClient("localhost:" + zkPort, 30000).zkProxyDir(zkPath).build().sync()
                .getNow();
        client = NedisUtils.newPooledClient(clientPool);
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        client.close().sync();
        client1.close().sync();
        client2.close().sync();
        if (redis1 != null) {
            redis1.stop();
        }
        if (redis2 != null) {
            redis2.stop();
        }
        if (zkServer != null) {
            zkServer.stop();
        }
        deleteDirectory(testDir);
    }

    @Test
    public void test() throws IOException, InterruptedException, KeeperException {
        assertTrue(client.set(toBytes("k1"), toBytes("v1")).sync().getNow().booleanValue());
        assertEquals("v1", bytesToString(client1.get(toBytes("k1")).sync().getNow()));
        // fake node
        addNode("node2", 12345, "offline");
        Thread.sleep(3000);
        assertTrue(client.set(toBytes("k2"), toBytes("v2")).sync().getNow().booleanValue());
        assertEquals("v2", bytesToString(client1.get(toBytes("k2")).sync().getNow()));

        addNode("node3", redisPort2, "online");
        Thread.sleep(3000);
        assertTrue(client.set(toBytes("k3"), toBytes("v3")).sync().getNow().booleanValue());
        assertEquals("v3", bytesToString(client2.get(toBytes("k3")).sync().getNow()));

        removeNode("node1");
        Thread.sleep(3000);
        assertTrue(client.set(toBytes("k4"), toBytes("v4")).sync().getNow().booleanValue());
        assertEquals("v4", bytesToString(client2.get(toBytes("k4")).sync().getNow()));

        System.out.println(clientPool.numConns());
        System.out.println(clientPool.numPooledConns());
    }
}
