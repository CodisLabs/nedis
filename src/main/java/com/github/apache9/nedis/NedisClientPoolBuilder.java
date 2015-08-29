package com.github.apache9.nedis;

import static com.github.apache9.nedis.NedisUtils.toBytes;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import org.apache.commons.lang3.tuple.Pair;

/**
 * @author Apache9
 */
public class NedisClientPoolBuilder {

    private static Pair<EventLoopGroup, Class<? extends Channel>> DEFAULT_EVENT_LOOP_CONFIG;

    private static synchronized Pair<EventLoopGroup, Class<? extends Channel>> defaultEventLoopConfig() {
        if (DEFAULT_EVENT_LOOP_CONFIG == null) {
            DEFAULT_EVENT_LOOP_CONFIG = Pair.<EventLoopGroup, Class<? extends Channel>>of(
                    new NioEventLoopGroup(), NioSocketChannel.class);
        }
        return DEFAULT_EVENT_LOOP_CONFIG;
    }

    private EventLoopGroup group;

    private Class<? extends Channel> channelClass;

    private long timeoutMs;

    private byte[] password;

    private byte[] database;

    private byte[] clientName;

    private int maxPooledConns = Math.max(2, 2 * ManagementFactory.getOperatingSystemMXBean()
            .getAvailableProcessors());

    private boolean exclusive;

    private SocketAddress remoteAddress;

    public NedisClientPoolBuilder group(EventLoopGroup group) {
        this.group = group;
        return this;
    }

    public NedisClientPoolBuilder channel(Class<? extends Channel> channelClass) {
        this.channelClass = channelClass;
        return this;
    }

    public NedisClientPoolBuilder timeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public NedisClientPoolBuilder password(String password) {
        this.password = toBytes(password);
        return this;
    }

    public NedisClientPoolBuilder database(int database) {
        this.database = toBytes(database);
        return this;
    }

    public NedisClientPoolBuilder clientName(String clientName) {
        this.clientName = toBytes(clientName);
        return this;
    }

    public NedisClientPoolBuilder maxPooledConns(int maxPooledConns) {
        this.maxPooledConns = maxPooledConns;
        return this;
    }

    public NedisClientPoolBuilder exclusive(boolean exclusive) {
        this.exclusive = exclusive;
        return this;
    }

    public NedisClientPoolBuilder remoteAddress(String host) {
        return remoteAddress(host, 6379);
    }

    public NedisClientPoolBuilder remoteAddress(String inetHost, int inetPort) {
        return remoteAddress(new InetSocketAddress(inetHost, inetPort));
    }

    public NedisClientPoolBuilder remoteAddress(SocketAddress remoteAddress) {
        this.remoteAddress = remoteAddress;
        return this;
    }

    private void validate() {
        if (group == null && channelClass != null) {
            throw new IllegalArgumentException("group is null but channel is not");
        }
        if (channelClass == null && group != null) {
            throw new IllegalArgumentException("channel is null but group is not");
        }
        if (group == null) {
            Pair<EventLoopGroup, Class<? extends Channel>> defaultEventLoopConfig = defaultEventLoopConfig();
            group = defaultEventLoopConfig.getLeft();
            channelClass = defaultEventLoopConfig.getRight();
        }
        if (remoteAddress == null) {
            throw new IllegalArgumentException("remoteAddress is not set");
        }
    }

    public NedisClientPool build() {
        validate();
        return new NedisClientPoolImpl(new Bootstrap().group(group).channel(channelClass)
                .remoteAddress(remoteAddress), timeoutMs, password, database, clientName,
                maxPooledConns, exclusive);
    }

    private NedisClientPoolBuilder() {}

    public static NedisClientPoolBuilder builder() {
        return new NedisClientPoolBuilder();
    }
}
