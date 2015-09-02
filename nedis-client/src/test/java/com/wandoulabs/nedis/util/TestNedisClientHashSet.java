package com.wandoulabs.nedis.util;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.wandoulabs.nedis.NedisClient;
import com.wandoulabs.nedis.util.NedisClientHashSet;

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
