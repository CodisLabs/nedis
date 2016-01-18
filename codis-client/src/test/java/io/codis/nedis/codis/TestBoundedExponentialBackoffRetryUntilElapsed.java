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

import static org.junit.Assert.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetrySleeper;
import org.junit.Test;

import io.codis.nedis.codis.BoundedExponentialBackoffRetryUntilElapsed;

/**
 * @author Apache9
 */
public class TestBoundedExponentialBackoffRetryUntilElapsed {

    private static final class FakeRetrySleeper implements RetrySleeper {

        public long sleepTimeMs;

        @Override
        public void sleepFor(long time, TimeUnit unit) {
            this.sleepTimeMs = unit.toMillis(time);
        }

    }

    @Test
    public void test() {
        FakeRetrySleeper sleeper = new FakeRetrySleeper();
        BoundedExponentialBackoffRetryUntilElapsed r = new BoundedExponentialBackoffRetryUntilElapsed(
                10, 2000, 60000);
        for (int i = 0; i < 100; i++) {
            assertTrue(r.allowRetry(i, ThreadLocalRandom.current().nextInt(60000), sleeper));
            System.out.println(sleeper.sleepTimeMs);
            assertTrue(sleeper.sleepTimeMs <= 2000);
        }
        assertTrue(r.allowRetry(1000, 59900, sleeper));
        System.out.println(sleeper.sleepTimeMs);
        assertTrue(sleeper.sleepTimeMs <= 100);
        sleeper.sleepTimeMs = -1L;
        assertFalse(r.allowRetry(1, 60000, sleeper));
        assertEquals(-1L, sleeper.sleepTimeMs);
    }
}
