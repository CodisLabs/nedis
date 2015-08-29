package com.github.apache9.nedis.protocol;

import io.netty.util.concurrent.Future;

import java.util.List;

/**
 * @author Apache9
 */
public interface BlockingListsCommands {
    
    Future<List<byte[]>> blpop(long timeoutSeconds, byte[]... keys);

    Future<List<byte[]>> brpop(long timeoutSeconds, byte[]... keys);

    Future<byte[]> brpoplpush(byte[] src, byte[] dst, long timeoutSeconds);
}
