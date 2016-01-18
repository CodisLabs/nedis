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
package io.codis.nedis.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import io.codis.nedis.NedisClient;
import io.codis.nedis.util.NedisClientHashSet;

/**
 * @author Apache9
 */
public class TestNedisClientHashSet {

    @Test
    public void test() {
        int cap = 1000;
        List<NedisClient> list = new ArrayList<>();
        NedisClientHashSet set = new NedisClientHashSet(cap);
        assertTrue(set.isEmpty());
        assertEquals(0, set.size());
        for (int i = 0; i < cap; i++) {
            NedisClient client = mock(NedisClient.class);
            list.add(client);
            assertEquals(i, set.size());
            assertFalse(set.contains(client));
            assertTrue(set.add(client));
            assertEquals(i + 1, set.size());
            assertTrue(set.contains(client));
            assertFalse(set.add(client));
            assertEquals(i + 1, set.size());
        }
        assertFalse(set.isEmpty());
        for (NedisClient client: list) {
            assertTrue(set.contains(client));
            assertTrue(set.remove(client));
            assertEquals(cap - 1, set.size());
            assertFalse(set.contains(client));
            assertTrue(set.add(client));
            assertEquals(cap, set.size());
        }
        for (NedisClient client: list) {
            assertSame(client, set.head(false));
            assertEquals(cap, set.size());
        }
        for (NedisClient client: list) {
            assertSame(client, set.head(false));
            assertEquals(cap, set.size());
        }
        int expectedSize = cap;
        for (NedisClient client: list) {
            assertSame(client, set.head(true));
            expectedSize--;
            assertEquals(expectedSize, set.size());
        }
        assertTrue(set.isEmpty());
        for (NedisClient client: list) {
            assertTrue(set.add(client));
        }
        NedisClient[] clients = set.toArray();
        assertEquals(cap, clients.length);
        for (int i = 0; i < cap; i++) {
            assertSame(list.get(i), clients[i]);
        }
    }
}
