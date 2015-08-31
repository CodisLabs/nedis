package com.github.apache9.nedis;

import static com.github.apache9.nedis.util.NedisUtils.bytesToString;
import static com.github.apache9.nedis.util.NedisUtils.newBytesMap;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.github.apache9.nedis.handler.RedisResponseDecoder;
import com.github.apache9.nedis.protocol.HashEntry;
import com.github.apache9.nedis.protocol.ScanResult;
import com.github.apache9.nedis.protocol.SortedSetEntry;

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

    public static PromiseConverter<List<byte[]>> toArray(EventExecutor executor) {
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
                            } else {
                                promise.trySuccess(Double.valueOf(bytesToString((byte[]) resp)));
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
                                List<List<byte[]>> rawValueList = (List<List<byte[]>>) list.get(1);
                                List<SortedSetEntry> values = new ArrayList<>(rawValueList.size());
                                for (List<byte[]> rawValue: rawValueList) {
                                    values.add(new SortedSetEntry(rawValue.get(0), Double
                                            .parseDouble(bytesToString(rawValue.get(1)))));
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
                                Map<byte[], byte[]> values = newBytesMap();
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
}
