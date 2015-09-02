package com.github.apache9.nedis.protocol;

import io.netty.util.concurrent.Future;

import java.util.List;

/**
 * @author Apache9
 */
public interface TransactionsCommands {

    public static final String QUEUED = "QUEUED";

    Future<Void> discard();

    Future<List<Object>> exec();

    Future<Void> multi();

    Future<Void> unwatch();

    Future<Void> watch(byte[]... keys);
}
