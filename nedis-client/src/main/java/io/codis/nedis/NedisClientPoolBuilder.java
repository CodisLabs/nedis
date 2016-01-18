/**
 * Copyright (c) 2015 CodisLabs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.codis.nedis;

import io.codis.nedis.util.AbstractNedisBuilder;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import static io.codis.nedis.util.NedisUtils.toBytes;

import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author Apache9
 */
public class NedisClientPoolBuilder extends AbstractNedisBuilder {

    private byte[] password;

    private int database;

    private byte[] clientName;

    private int maxPooledConns = Math.max(2, 2 * ManagementFactory.getOperatingSystemMXBean()
            .getAvailableProcessors());

    private boolean exclusive;

    private SocketAddress remoteAddress;

    @Override
    public NedisClientPoolBuilder group(EventLoopGroup group) {
        super.group(group);
        return this;
    }

    @Override
    public NedisClientPoolBuilder channel(Class<? extends Channel> channelClass) {
        super.channel(channelClass);
        return this;
    }

    @Override
    public NedisClientPoolBuilder timeoutMs(long timeoutMs) {
        super.timeoutMs(timeoutMs);
        return this;
    }

    public NedisClientPoolBuilder password(String password) {
        this.password = toBytes(password);
        return this;
    }

    public NedisClientPoolBuilder database(int database) {
        this.database = database;
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

    public boolean exclusive() {
        return exclusive;
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
        validateGroupConfig();
        if (remoteAddress == null) {
            throw new IllegalArgumentException("remoteAddress is not set");
        }
    }

    public NedisClientPoolImpl build() {
        validate();
        return new NedisClientPoolImpl(group, channelClass, timeoutMs, remoteAddress, password,
                database, clientName, maxPooledConns, exclusive);
    }

    public static NedisClientPoolBuilder create() {
        return new NedisClientPoolBuilder();
    }
}
