package com.github.apache9.nedis.protocol;

import io.netty.util.concurrent.Future;

import java.util.List;

/**
 * @author Apache9
 */
public interface ServerCommands {

    Future<Void> bgrewriteaof();

    Future<Void> bgsave();

    Future<byte[]> clientGetname();

    Future<Void> clientKill(byte[] addr);

    Future<byte[]> clientList();

    Future<Void> clientSetname(byte[] name);

    Future<List<byte[]>> configGet(byte[] pattern);

    Future<Void> configResetstat();

    Future<Void> configRewrite();

    Future<Void> configSet(byte[] name, byte[] value);

    Future<Long> dbsize();

    Future<Void> flushall();

    Future<Void> flushdb();

    Future<byte[]> info();

    Future<byte[]> info(byte[] section);

    Future<Long> lastsave();

    Future<List<byte[]>> role();

    Future<Void> save(boolean save);

    Future<Void> slaveof(String host, int port);

    Future<Void> sync();

    Future<List<byte[]>> time();
}
