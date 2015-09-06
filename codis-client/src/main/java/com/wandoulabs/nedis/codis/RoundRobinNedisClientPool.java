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
package com.wandoulabs.nedis.codis;

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_ADDED;
import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_REMOVED;
import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_UPDATED;
import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.INITIALIZED;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wandoulabs.nedis.NedisClient;
import com.wandoulabs.nedis.NedisClientPool;
import com.wandoulabs.nedis.NedisClientPoolBuilder;

/**
 * @author Apache9
 */
public class RoundRobinNedisClientPool implements NedisClientPool {

    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinNedisClientPool.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String CODIS_PROXY_STATE_ONLINE = "online";

    private static final EnumSet<PathChildrenCacheEvent.Type> RESET_TYPES = EnumSet.of(CHILD_ADDED,
            CHILD_UPDATED, CHILD_REMOVED);

    private static final class PooledObject {
        public final String addr;

        public final NedisClientPool pool;

        public PooledObject(String addr, NedisClientPool pool) {
            this.addr = addr;
            this.pool = pool;
        }

    }

    private volatile List<PooledObject> pools = Collections.emptyList();

    private final CuratorFramework curatorClient;

    private final boolean closeCurator;

    private final PathChildrenCache watcher;

    private final NedisClientPoolBuilder poolBuilder;

    private final AtomicInteger nextIdx = new AtomicInteger(-1);

    private final Promise<Void> closeFuture;

    private final Promise<RoundRobinNedisClientPool> initFuture;

    private RoundRobinNedisClientPool(CuratorFramework curatorClient, boolean closeCurator,
            String zkProxyDir, NedisClientPoolBuilder poolBuilder) throws Exception {
        this.curatorClient = curatorClient;
        this.closeCurator = closeCurator;
        this.poolBuilder = poolBuilder;
        EventLoop eventLoop = poolBuilder.group().next();
        this.closeFuture = eventLoop.newPromise();
        this.initFuture = eventLoop.newPromise();
        watcher = new PathChildrenCache(curatorClient, zkProxyDir, true);
        watcher.getListenable().addListener(new PathChildrenCacheListener() {

            private boolean initialized = false;

            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
                    throws Exception {
                StringBuilder sb = new StringBuilder("Zookeeper event received: type=")
                        .append(event.getType());
                if (event.getData() != null) {
                    ChildData data = event.getData();
                    sb.append(", path=").append(data.getPath()).append(", stat=")
                            .append(data.getStat());
                }
                LOG.info(sb.toString());
                if (!initialized) {
                    if (event.getType() == INITIALIZED) {
                        resetPools();
                        initFuture.trySuccess(RoundRobinNedisClientPool.this);
                        initialized = true;
                    }
                } else if (RESET_TYPES.contains(event.getType())) {
                    resetPools();
                }
            }
        });
        watcher.start(StartMode.POST_INITIALIZED_EVENT);
    }

    private void resetPools() {
        List<PooledObject> oldPools = this.pools;
        Map<String, PooledObject> addr2Pool = new HashMap<>(oldPools.size());
        for (PooledObject pool: oldPools) {
            addr2Pool.put(pool.addr, pool);
        }
        List<PooledObject> newPools = new ArrayList<>();
        for (ChildData childData: watcher.getCurrentData()) {
            try {
                CodisProxyInfo proxyInfo = MAPPER.readValue(childData.getData(),
                        CodisProxyInfo.class);
                if (!CODIS_PROXY_STATE_ONLINE.equals(proxyInfo.getState())) {
                    continue;
                }
                String addr = proxyInfo.getAddr();
                PooledObject pool = addr2Pool.remove(addr);
                if (pool == null) {
                    LOG.info("Add new proxy: " + addr);
                    String[] hostAndPort = addr.split(":");
                    String host = hostAndPort[0];
                    int port = Integer.parseInt(hostAndPort[1]);
                    pool = new PooledObject(addr, poolBuilder.remoteAddress(host, port).build());
                }
                newPools.add(pool);
            } catch (Exception e) {
                LOG.warn("parse " + childData.getPath() + " failed", e);
            }
        }
        this.pools = newPools;
        for (PooledObject pool: addr2Pool.values()) {
            LOG.info("Remove proxy: " + pool.addr);
            pool.pool.close();
        }
    }

    public Future<RoundRobinNedisClientPool> initFuture() {
        return initFuture;
    }

    @Override
    public Future<Void> closeFuture() {
        return closeFuture;
    }

    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public Future<Void> close() {
        if (!closed.compareAndSet(false, true)) {
            return closeFuture;
        }
        new Thread(getClass().getSimpleName() + "-Closer") {

            @Override
            public void run() {
                try {
                    watcher.close();
                } catch (IOException e) {
                    LOG.warn("IOException should not have been thrown", e);
                }
                if (closeCurator) {
                    curatorClient.close();
                }
                List<PooledObject> toClose = pools;
                final AtomicInteger numOpenPool = new AtomicInteger(toClose.size());
                for (PooledObject pool: toClose) {
                    pool.pool.close().addListener(new FutureListener<Void>() {

                        @Override
                        public void operationComplete(Future<Void> future) throws Exception {
                            if (numOpenPool.decrementAndGet() == 0) {
                                closeFuture.trySuccess(null);
                            }
                        }
                    });
                }
            }

        }.start();

        return closeFuture;
    }

    @Override
    public Future<NedisClient> acquire() {
        List<PooledObject> pools = this.pools;
        if (pools.isEmpty()) {
            return poolBuilder.group().next().newFailedFuture(new IOException("Proxy list empty"));
        }
        for (;;) {
            int current = nextIdx.get();
            int next = current >= pools.size() - 1 ? 0 : current + 1;
            if (nextIdx.compareAndSet(current, next)) {
                return pools.get(next).pool.acquire();
            }
        }
    }

    @Override
    public void release(NedisClient client) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exclusive() {
        return poolBuilder.exclusive();
    }

    @Override
    public int numConns() {
        int numConns = 0;
        for (PooledObject pool: pools) {
            numConns += pool.pool.numConns();
        }
        return numConns;
    }

    @Override
    public int numPooledConns() {
        int numPooledConns = 0;
        for (PooledObject pool: pools) {
            numPooledConns += pool.pool.numPooledConns();
        }
        return numPooledConns;
    }

    public static class Builder {

        private static final int CURATOR_RETRY_BASE_SLEEP_MS = 100;

        private static final int CURATOR_RETRY_MAX_SLEEP_MS = 30 * 1000;

        private NedisClientPoolBuilder poolBuilder;

        private CuratorFramework curatorClient;

        private boolean closeCurator;

        private String zkProxyDir;

        private String zkAddr;

        private int zkSessionTimeoutMs;

        private Builder() {}

        public Builder poolBuilder(NedisClientPoolBuilder poolBuilder) {
            this.poolBuilder = poolBuilder;
            return this;
        }

        public Builder curatorClient(CuratorFramework curatorClient, boolean closeCurator) {
            this.curatorClient = curatorClient;
            this.closeCurator = closeCurator;
            return this;
        }

        public Builder zkProxyDir(String zkProxyDir) {
            this.zkProxyDir = zkProxyDir;
            return this;
        }

        public Builder curatorClient(String zkAddr, int zkSessionTimeoutMs) {
            this.zkAddr = zkAddr;
            this.zkSessionTimeoutMs = zkSessionTimeoutMs;
            return this;
        }

        private void validate() {
            poolBuilder.createGroupIfNecessary();
            if (zkProxyDir == null) {
                throw new IllegalArgumentException("zkProxyDir can not be null");
            }
            if (curatorClient == null) {
                if (zkAddr == null) {
                    throw new IllegalArgumentException("zk client can not be null");
                }
                curatorClient = CuratorFrameworkFactory
                        .builder()
                        .connectString(zkAddr)
                        .sessionTimeoutMs(zkSessionTimeoutMs)
                        .retryPolicy(
                                new BoundedExponentialBackoffRetryUntilElapsed(
                                        CURATOR_RETRY_BASE_SLEEP_MS, CURATOR_RETRY_MAX_SLEEP_MS,
                                        -1L)).build();
                curatorClient.start();
                closeCurator = true;
            }
        }

        public Future<RoundRobinNedisClientPool> build() {
            validate();
            try {
                return new RoundRobinNedisClientPool(curatorClient, closeCurator, zkProxyDir,
                        poolBuilder).initFuture();
            } catch (Exception e) {
                return poolBuilder.group().next().newFailedFuture(e);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
