package com.wandoulabs.nedis.protocol;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface HyperLogLogCommands {

    Future<Boolean> pfadd(byte[] key, byte[]... elements);

    Future<Long> pfcount(byte[]... keys);

    Future<Void> pfmerge(byte[] dst, byte[]... keys);
}
