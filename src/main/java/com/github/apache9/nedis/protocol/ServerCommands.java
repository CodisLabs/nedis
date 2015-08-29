package com.github.apache9.nedis.protocol;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface ServerCommands {

    Future<Void> clientSetname(byte[] name);
}
