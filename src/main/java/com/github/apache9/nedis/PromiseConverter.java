package com.github.apache9.nedis;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.util.List;

import com.github.apache9.nedis.handler.RedisResponseDecoder;

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
                                promise.trySuccess(Double.valueOf(resp.toString()));
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
}
