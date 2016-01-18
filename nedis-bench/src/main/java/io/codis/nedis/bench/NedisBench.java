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

import io.codis.nedis.NedisClient;
import io.codis.nedis.NedisClientPoolBuilder;
import io.codis.nedis.util.NedisUtils;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author Apache9
 */
public class NedisBench {

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

        private final NedisClient client;

        private final int pipeline;

        public Worker(AtomicBoolean stop, AtomicLong reqCount, NedisClient client, int pipeline) {
            this.stop = stop;
            this.reqCount = reqCount;
            this.client = client;
            this.pipeline = pipeline;
        }

        private void testAsync(byte[] key, byte[] value) {
            final Semaphore concurrencyControl = new Semaphore(pipeline);
            FutureListener<Object> listener = new FutureListener<Object>() {

                @Override
                public void operationComplete(Future<Object> future) throws Exception {
                    if (future.isSuccess()) {
                        concurrencyControl.release();
                        reqCount.incrementAndGet();
                    } else {
                        future.cause().printStackTrace();
                        System.exit(1);
                    }
                }
            };
            while (!stop.get()) {
                concurrencyControl.acquireUninterruptibly(2);
                client.set(key, value).addListener(listener);
                client.get(key).addListener(listener);
            }
        }

        private void testSync(byte[] key, byte[] value) {
            while (!stop.get()) {
                try {
                    client.set(key, value).sync();
                    reqCount.incrementAndGet();
                    client.get(key).sync();
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
                testAsync(key, value);
            } else {
                testSync(key, value);
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
        NedisClient client = NedisUtils.newPooledClient(NedisClientPoolBuilder.create()
                .maxPooledConns(parsedArgs.conns).remoteAddress(hap.getHostText(), hap.getPort())
                .build());
        for (int i = 0; i < parsedArgs.threads; i++) {
            executor.execute(new Worker(stop, reqCount, client, parsedArgs.pipeline));
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
                client.close().sync();
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
