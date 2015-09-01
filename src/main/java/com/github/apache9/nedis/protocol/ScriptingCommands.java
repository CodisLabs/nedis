package com.github.apache9.nedis.protocol;

import io.netty.util.concurrent.Future;

import java.util.List;

/**
 * @author Apache9
 */
public interface ScriptingCommands {

    Future<Object> eval(byte[] script, int numKeys, byte[]... keysvalues);

    Future<Object> evalsha(byte[] sha1, int numKeys, byte[]... keysvalues);

    Future<List<Boolean>> scriptExists(byte[]... scripts);

    Future<Void> scriptFlush();

    Future<Void> scriptKill();

    Future<byte[]> scriptLoad(byte[] script);
}
