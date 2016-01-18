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
package io.codis.nedis.util;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import static io.codis.nedis.util.NedisUtils.defaultEventLoopConfig;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Common configurations:
 * <ul>
 * <li>EventLoopGroup</li>
 * <li>ChannelClass</li>
 * <li>TimeoutMs</li>
 * </ul>
 * 
 * @author Apache9
 */
public class AbstractNedisBuilder {

    protected EventLoopGroup group;

    protected Class<? extends Channel> channelClass;

    protected long timeoutMs;

    public AbstractNedisBuilder group(EventLoopGroup group) {
        this.group = group;
        return this;
    }

    public EventLoopGroup group() {
        return group;
    }

    public AbstractNedisBuilder channel(Class<? extends Channel> channelClass) {
        this.channelClass = channelClass;
        return this;
    }

    public AbstractNedisBuilder timeoutMs(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public final void validateGroupConfig() {
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
    }
}
