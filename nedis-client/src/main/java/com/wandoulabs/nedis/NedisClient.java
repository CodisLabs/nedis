/**
 * Copyright (c) 2015 Wandoujia Inc.
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
package com.wandoulabs.nedis;

import io.netty.util.concurrent.Future;

import com.wandoulabs.nedis.protocol.ConnectionCommands;
import com.wandoulabs.nedis.protocol.HashesCommands;
import com.wandoulabs.nedis.protocol.HyperLogLogCommands;
import com.wandoulabs.nedis.protocol.KeysCommands;
import com.wandoulabs.nedis.protocol.ListsCommands;
import com.wandoulabs.nedis.protocol.ScriptingCommands;
import com.wandoulabs.nedis.protocol.ServerCommands;
import com.wandoulabs.nedis.protocol.SetsCommands;
import com.wandoulabs.nedis.protocol.SortedSetsCommands;
import com.wandoulabs.nedis.protocol.StringsCommands;
import com.wandoulabs.nedis.protocol.TransactionsCommands;

/**
 * @author Apache9
 */
public interface NedisClient extends ConnectionManagement, ConnectionCommands, KeysCommands,
        StringsCommands, ScriptingCommands, ListsCommands, SetsCommands, ServerCommands,
        HashesCommands, HyperLogLogCommands, SortedSetsCommands, TransactionsCommands {

    /**
     * General method to execute a redis command.
     */
    Future<Object> execCmd(byte[] cmd, byte[]... params);
}
