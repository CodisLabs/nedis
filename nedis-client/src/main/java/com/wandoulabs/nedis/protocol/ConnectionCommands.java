package com.wandoulabs.nedis.protocol;

import io.netty.util.concurrent.Future;

/**
 * @author zhangduo
 */
public interface ConnectionCommands {

    Future<Void> auth(byte[] password);

    Future<byte[]> echo(byte[] msg);

    Future<String> ping();

    Future<Void> quit();

    Future<Void> select(int index);
}
