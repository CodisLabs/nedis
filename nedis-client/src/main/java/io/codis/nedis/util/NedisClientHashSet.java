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

import io.codis.nedis.NedisClient;

/**
 * An identity LinkedHashSet.
 * 
 * @author Apache9
 */
public class NedisClientHashSet {

    private static final float LOAD_FACTOR = 0.75f;

    private static final int MAXIMUM_CAPACITY = 1 << 30;

    private static final class Entry {

        final NedisClient value;

        Entry next;

        Entry before;

        Entry after;

        public Entry(NedisClient value, Entry next) {
            this.value = value;
            this.next = next;
        }

        public void remove() {
            before.after = after;
            after.before = before;
        }

        public void addBefore(Entry existingEntry) {
            after = existingEntry;
            before = existingEntry.before;
            before.after = this;
            after.before = this;
        }
    }

    private final Entry[] table;

    private Entry header;

    private int size;

    private static final int tableSizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    private int indexFor(NedisClient client) {
        int h = System.identityHashCode(client);
        h = h ^ (h >>> 16);
        return h & (table.length - 1);
    }

    public NedisClientHashSet(int size) {
        table = new Entry[tableSizeFor((int) (size / LOAD_FACTOR))];
        header = new Entry(null, null);
        header.before = header.after = header;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    public boolean add(NedisClient value) {
        int i = indexFor(value);
        for (Entry e = table[i]; e != null; e = e.next) {
            if (e.value == value) {
                return false;
            }
        }
        Entry old = table[i];
        Entry e = new Entry(value, old);
        table[i] = e;
        e.addBefore(header);
        size++;
        return true;
    }

    public boolean contains(NedisClient value) {
        if (isEmpty()) {
            return false;
        }
        int i = indexFor(value);
        for (Entry e = table[i]; e != null; e = e.next) {
            if (e.value == value) {
                return true;
            }
        }
        return false;
    }

    public boolean remove(NedisClient value) {
        if (isEmpty()) {
            return false;
        }
        int i = indexFor(value);
        Entry prev = table[i];
        Entry e = prev;

        while (e != null) {
            Entry next = e.next;
            if (value == e.value) {
                size--;
                if (prev == e) {
                    table[i] = next;
                } else {
                    prev.next = next;
                }
                e.remove();
                return true;
            }
            prev = e;
            e = next;
        }

        return false;
    }

    public NedisClient head(boolean remove) {
        if (isEmpty()) {
            return null;
        }
        Entry e = header.after;
        if (remove) {
            remove(e.value);
        } else {
            e.remove();
            e.addBefore(header);
        }
        return e.value;
    }

    public NedisClient[] toArray() {
        NedisClient[] arr = new NedisClient[size];
        Entry e = header.after;
        for (int i = 0; i < size; i++) {
            arr[i] = e.value;
            e = e.after;
        }
        return arr;
    }
}
