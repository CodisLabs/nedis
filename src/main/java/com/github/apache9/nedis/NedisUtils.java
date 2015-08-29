package com.github.apache9.nedis;

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.github.apache9.nedis.protocol.BlockingListsCommands;

/**
 * @author Apache9
 */
public class NedisUtils {

    public static byte[] toBytes(double value) {
        return toBytes(Double.toString(value));
    }

    public static byte[] toBytes(int value) {
        return toBytes(Integer.toString(value));
    }

    public static byte[] toBytes(long value) {
        return toBytes(Long.toString(value));
    }

    public static byte[] toBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String toString(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    public static byte[][] toParamsReverse(byte[][] tailParams, byte[]... headParams) {
        return toParams(headParams, tailParams);
    }

    public static byte[][] toParams(byte[][] headParams, byte[]... tailParams) {
        byte[][] params = Arrays.copyOf(headParams, headParams.length + tailParams.length);
        System.arraycopy(tailParams, 0, params, headParams.length, tailParams.length);
        return params;
    }

    private static EventExecutor getEventExecutor(Future<?> future) {
        Class<?> clazz = future.getClass();
        for (;;) {
            try {
                Method method = clazz.getDeclaredMethod("executor");
                if (method != null) {
                    method.setAccessible(true);
                    return (EventExecutor) method.invoke(future);
                }
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                    | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            clazz = clazz.getSuperclass();
            if (clazz == null) {
                return null;
            }
        }
    }

    private static final class Invoker implements InvocationHandler {

        private final NedisClientPool pool;

        public Invoker(NedisClientPool pool) {
            this.pool = pool;
        }

        private void resetTimeout(final NedisClient client, long previousTimeoutMs) {
            client.setTimeout(previousTimeoutMs).addListener(new FutureListener<Long>() {

                @Override
                public void operationComplete(Future<Long> future) throws Exception {
                    if (future.getNow() != null && pool.exclusive()) {
                        pool.release(client);
                    }
                }

            });
        }

        @SuppressWarnings({
            "unchecked", "rawtypes"
        })
        private void callBlocking(final NedisClient client, Method method, Object[] args,
                final long previousTimeoutMs, final Promise promise) throws IllegalAccessException,
                InvocationTargetException {
            ((Future) method.invoke(client, args)).addListener(new FutureListener() {

                @Override
                public void operationComplete(Future future) throws Exception {
                    if (future.isSuccess()) {
                        promise.trySuccess(future.getNow());
                    } else {
                        promise.tryFailure(future.cause());
                    }
                    resetTimeout(client, previousTimeoutMs);
                }
            });
        }

        private void setInfiniteTimeout(final NedisClient client, final Method method,
                final Object[] args, @SuppressWarnings("rawtypes") final Promise promise) {
            client.setTimeout(0L).addListener(new FutureListener<Long>() {

                @Override
                public void operationComplete(Future<Long> future) throws Exception {
                    // will not fail, but could be null
                    Long previousTimeoutMs = future.get();
                    if (previousTimeoutMs == null) {
                        promise.tryFailure(new IllegalStateException("already closed"));
                    } else {
                        callBlocking(client, method, args, previousTimeoutMs.longValue(), promise);
                    }
                }
            });
        }

        @SuppressWarnings({
            "unchecked", "rawtypes"
        })
        private void call(final NedisClient client, Method method, Object[] args,
                final Promise promise) throws IllegalAccessException, InvocationTargetException {
            ((Future) method.invoke(client, args)).addListener(new FutureListener() {

                @Override
                public void operationComplete(Future future) throws Exception {
                    if (future.isSuccess()) {
                        promise.trySuccess(future.getNow());
                    } else {
                        promise.tryFailure(future.cause());
                    }
                    if (pool.exclusive()) {
                        pool.release(client);
                    }
                }
            });
        }

        @Override
        public Object invoke(Object proxy, final Method method, final Object[] args)
                throws Throwable {
            // delegate Object methods like toString, hashCode, etc.
            if (method.getDeclaringClass().equals(Object.class)) {
                return method.invoke(this, args);
            }
            Future<NedisClient> clientFuture = pool.acquire();
            @SuppressWarnings("rawtypes")
            final Promise promise = getEventExecutor(clientFuture).newPromise();
            clientFuture.addListener(new FutureListener<NedisClient>() {

                @Override
                public void operationComplete(Future<NedisClient> future) throws Exception {
                    if (future.isSuccess()) {
                        if (method.getDeclaringClass().equals(BlockingListsCommands.class)) {
                            setInfiniteTimeout(future.getNow(), method, args, promise);
                        } else {
                            call(future.getNow(), method, args, promise);
                        }
                    } else {
                        promise.tryFailure(future.cause());
                    }
                }
            });
            return promise;
        }

    }

    public static NedisClient newPooledClient(NedisClientPool pool) {
        return (NedisClient) Proxy.newProxyInstance(NedisClient.class.getClassLoader(),
                new Class<?>[] {
                    NedisClient.class
                }, new Invoker(pool));
    }
}
