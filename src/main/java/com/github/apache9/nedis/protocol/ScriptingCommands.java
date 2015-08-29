package com.github.apache9.nedis.protocol;

import io.netty.util.concurrent.Future;

/**
 * @author Apache9
 */
public interface ScriptingCommands {

    Future<Object> eval(byte[] script, int numKeys, byte[]... keysvalues);
}
