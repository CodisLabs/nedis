package com.github.apache9.nedis.protocol;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface KeysCommands {

    Future<Long> del(byte[]... keys);

    Future<Boolean> exists(byte[] key);

    Future<Boolean> expire(byte[] key, long seconds);

    Future<Boolean> expireAt(byte[] key, long unixTime);

}
