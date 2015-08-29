package com.github.apache9.nedis.protocol;

import io.netty.util.concurrent.Future;

/**
 * @author zhangduo
 */
public interface ConnectionCommands {

    Future<byte[]> echo(byte[] msg);

    Future<String> ping();
}
