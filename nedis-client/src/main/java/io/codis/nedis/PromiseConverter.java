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

import io.codis.nedis.exception.RedisResponseException;
import io.codis.nedis.handler.RedisResponseDecoder;
import io.codis.nedis.protocol.HashEntry;
import io.codis.nedis.protocol.ScanResult;
import io.codis.nedis.protocol.SortedSetEntry;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import static io.codis.nedis.util.NedisUtils.bytesToDouble;
import static io.codis.nedis.util.NedisUtils.newBytesKeyMap;
import static io.codis.nedis.util.NedisUtils.newBytesSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Convert redis response to give type.
 * 
 * @author Apache9
 */
abstract class PromiseConverter<T> {

    private final EventExecutor executor;

    public PromiseConverter(EventExecutor executor) {
        this.executor = executor;
    }

    public abstract FutureListener<Object> newListener(Promise<T> promise);

    public Promise<T> newPromise() {
        return executor.newPromise();
    }

    public static PromiseConverter<List<byte[]>> toList(EventExecutor executor) {
        return new PromiseConverter<List<byte[]>>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<List<byte[]>> promise) {
                return new FutureListener<Object>() {

                    @SuppressWarnings("unchecked")
                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(null);
                            } else {
                                promise.trySuccess((List<byte[]>) resp);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<Boolean> toBoolean(EventExecutor executor) {
        return new PromiseConverter<Boolean>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<Boolean> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(false);
                            } else if (resp instanceof String) {
                                promise.trySuccess(true);
                            } else {
                                promise.trySuccess(((Long) resp).intValue() != 0);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<byte[]> toBytes(EventExecutor executor) {
        return new PromiseConverter<byte[]>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<byte[]> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(null);
                            } else {
                                promise.trySuccess((byte[]) resp);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<Double> toDouble(EventExecutor executor) {
        return new PromiseConverter<Double>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<Double> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(null);
                            } else {
                                promise.trySuccess(bytesToDouble((byte[]) resp));
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<Long> toLong(EventExecutor executor) {
        return new PromiseConverter<Long>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<Long> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(null);
                            } else {
                                promise.trySuccess((Long) resp);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<Object> toObject(EventExecutor executor) {
        return new PromiseConverter<Object>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<Object> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(null);
                            } else {
                                promise.trySuccess(resp);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<String> toString(EventExecutor executor) {
        return new PromiseConverter<String>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<String> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(null);
                            } else {
                                promise.trySuccess(resp.toString());
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<Void> toVoid(EventExecutor executor) {
        return new PromiseConverter<Void>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<Void> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else {
                                promise.trySuccess(null);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<ScanResult<byte[]>> toArrayScanResult(EventExecutor executor) {
        return new PromiseConverter<ScanResult<byte[]>>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<ScanResult<byte[]>> promise) {
                return new FutureListener<Object>() {

                    @SuppressWarnings("unchecked")
                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else {
                                List<Object> list = (List<Object>) resp;
                                promise.trySuccess(new ScanResult<byte[]>((byte[]) list.get(0),
                                        (List<byte[]>) list.get(1)));
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<ScanResult<HashEntry>> toHashScanResult(EventExecutor executor) {
        return new PromiseConverter<ScanResult<HashEntry>>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<ScanResult<HashEntry>> promise) {
                return new FutureListener<Object>() {

                    @SuppressWarnings("unchecked")
                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else {
                                List<Object> list = (List<Object>) resp;
                                byte[] cursor = (byte[]) list.get(0);
                                List<byte[]> rawValueList = (List<byte[]>) list.get(1);
                                List<HashEntry> values = new ArrayList<>(rawValueList.size() / 2);
                                for (Iterator<byte[]> iter = rawValueList.iterator(); iter
                                        .hasNext();) {
                                    values.add(new HashEntry(iter.next(), iter.next()));
                                }
                                promise.trySuccess(new ScanResult<HashEntry>(cursor, values));
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<ScanResult<SortedSetEntry>> toSortedSetScanResult(
            EventExecutor executor) {
        return new PromiseConverter<ScanResult<SortedSetEntry>>(executor) {

            @Override
            public FutureListener<Object> newListener(
                    final Promise<ScanResult<SortedSetEntry>> promise) {
                return new FutureListener<Object>() {

                    @SuppressWarnings("unchecked")
                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else {
                                List<Object> list = (List<Object>) resp;
                                byte[] cursor = (byte[]) list.get(0);
                                List<byte[]> rawValueList = (List<byte[]>) list.get(1);
                                List<SortedSetEntry> values = new ArrayList<>(
                                        rawValueList.size() / 2);
                                for (Iterator<byte[]> iter = rawValueList.iterator(); iter
                                        .hasNext();) {
                                    values.add(new SortedSetEntry(iter.next(), bytesToDouble(iter
                                            .next())));
                                }
                                promise.trySuccess(new ScanResult<SortedSetEntry>(cursor, values));
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<Map<byte[], byte[]>> toMap(EventExecutor executor) {
        return new PromiseConverter<Map<byte[], byte[]>>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<Map<byte[], byte[]>> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else {
                                @SuppressWarnings("unchecked")
                                List<byte[]> rawValueList = (List<byte[]>) resp;
                                Map<byte[], byte[]> values = newBytesKeyMap();
                                for (Iterator<byte[]> iter = rawValueList.iterator(); iter
                                        .hasNext();) {
                                    values.put(iter.next(), iter.next());
                                }
                                promise.trySuccess(values);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<Set<byte[]>> toSet(EventExecutor executor) {
        return new PromiseConverter<Set<byte[]>>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<Set<byte[]>> promise) {
                return new FutureListener<Object>() {

                    @SuppressWarnings("unchecked")
                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(null);
                            } else {
                                Set<byte[]> values = newBytesSet();
                                values.addAll((List<byte[]>) resp);
                                promise.trySuccess(values);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<List<SortedSetEntry>> toSortedSetEntryList(EventExecutor executor) {
        return new PromiseConverter<List<SortedSetEntry>>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<List<SortedSetEntry>> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else {
                                @SuppressWarnings("unchecked")
                                List<byte[]> rawValueList = (List<byte[]>) resp;
                                List<SortedSetEntry> values = new ArrayList<>(
                                        rawValueList.size() / 2);
                                for (Iterator<byte[]> iter = rawValueList.iterator(); iter
                                        .hasNext();) {
                                    values.add(new SortedSetEntry(iter.next(), bytesToDouble(iter
                                            .next())));
                                }
                                promise.trySuccess(values);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<List<Boolean>> toBooleanList(EventExecutor executor) {
        return new PromiseConverter<List<Boolean>>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<List<Boolean>> promise) {
                return new FutureListener<Object>() {

                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(null);
                            } else {
                                @SuppressWarnings("unchecked")
                                List<Long> rawValueList = (List<Long>) resp;
                                List<Boolean> values = new ArrayList<>(rawValueList.size());
                                for (long l: rawValueList) {
                                    values.add(l != 0L);
                                }
                                promise.trySuccess(values);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }

    public static PromiseConverter<List<Object>> toObjectList(EventExecutor executor) {
        return new PromiseConverter<List<Object>>(executor) {

            @Override
            public FutureListener<Object> newListener(final Promise<List<Object>> promise) {
                return new FutureListener<Object>() {

                    @SuppressWarnings("unchecked")
                    @Override
                    public void operationComplete(Future<Object> future) throws Exception {
                        if (future.isSuccess()) {
                            Object resp = future.getNow();
                            if (resp instanceof RedisResponseException) {
                                promise.tryFailure((RedisResponseException) resp);
                            } else if (resp == RedisResponseDecoder.NULL_REPLY) {
                                promise.trySuccess(null);
                            } else {
                                promise.trySuccess((List<Object>) resp);
                            }
                        } else {
                            promise.tryFailure(future.cause());
                        }
                    }
                };
            }
        };
    }
}
