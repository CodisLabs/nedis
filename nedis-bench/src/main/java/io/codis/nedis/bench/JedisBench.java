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
package io.codis.nedis.bench;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author Apache9
 */
public class JedisBench {

    private static class Args {

        @Option(name = "-c", required = true)
        public int conns;

        @Option(name = "-pl")
        public int pipeline;

        @Option(name = "-t", required = true)
        public int threads;

        @Option(name = "-m")
        public long minutes = TimeUnit.DAYS.toMinutes(1);

        @Option(name = "-r", required = true)
        public String redisAddr;
    }

    private static final class Worker implements Runnable {

        private final AtomicBoolean stop;

        private final AtomicLong reqCount;

        private final JedisPool pool;

        private final int pipeline;

        public Worker(AtomicBoolean stop, AtomicLong reqCount, JedisPool pool, int pipeline) {
            this.stop = stop;
            this.reqCount = reqCount;
            this.pool = pool;
            this.pipeline = pipeline;
        }

        private void testPipeline(byte[] key, byte[] value) {
            while (!stop.get()) {
                try (Jedis jedis = pool.getResource()) {
                    Pipeline p = jedis.pipelined();
                    for (int i = 0; i < pipeline / 2; i++) {
                        p.set(key, value);
                        p.get(key);
                    }
                    p.sync();
                    reqCount.addAndGet(pipeline);
                }
            }
        }

        private void testNormal(byte[] key, byte[] value) {
            while (!stop.get()) {
                try {
                    try (Jedis jedis = pool.getResource()) {
                        jedis.set(key, value);
                    }
                    reqCount.incrementAndGet();
                    try (Jedis jedis = pool.getResource()) {
                        jedis.get(key);
                    }
                    reqCount.incrementAndGet();
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.exit(1);
                }
            }
        }

        @Override
        public void run() {
            byte[] key = new byte[4];
            byte[] value = new byte[16];
            ThreadLocalRandom.current().nextBytes(key);
            ThreadLocalRandom.current().nextBytes(value);
            if (pipeline > 0) {
                testPipeline(key, value);
            } else {
                testNormal(key, value);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Args parsedArgs = new Args();
        CmdLineParser parser = new CmdLineParser(parsedArgs);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            return;
        }

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong reqCount = new AtomicLong(0);
        ExecutorService executor = Executors.newFixedThreadPool(parsedArgs.threads,
                new ThreadFactoryBuilder().setDaemon(true).build());
        HostAndPort hap = HostAndPort.fromString(parsedArgs.redisAddr);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(parsedArgs.conns);
        config.setMaxIdle(parsedArgs.conns);
        JedisPool pool = new JedisPool(config, hap.getHostText(), hap.getPort());
        for (int i = 0; i < parsedArgs.threads; i++) {
            executor.execute(new Worker(stop, reqCount, pool, parsedArgs.pipeline));
        }
        long duration = TimeUnit.MINUTES.toNanos(parsedArgs.minutes);
        long startTime = System.nanoTime();
        long prevTime = -1L;
        long prevReqCount = -1L;
        for (;;) {
            long currentTime = System.nanoTime();
            if (currentTime - startTime >= duration) {
                stop.set(true);
                executor.shutdown();
                if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                    throw new RuntimeException("Can not terminate workers");
                }
                System.out.println(String.format("Test run %d minutes, qps: %.2f",
                        parsedArgs.minutes, (double) reqCount.get() / (currentTime - startTime)
                                * TimeUnit.SECONDS.toNanos(1)));
                pool.close();
                return;
            }
            long currentReqCount = reqCount.get();
            if (prevTime > 0) {
                System.out.println(String.format("qps: %.2f",
                        (double) (currentReqCount - prevReqCount) / (currentTime - prevTime)
                                * TimeUnit.SECONDS.toNanos(1)));
            }
            prevTime = currentTime;
            prevReqCount = currentReqCount;
            Thread.sleep(5000);
        }
    }
}
