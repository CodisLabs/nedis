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

import io.codis.nedis.protocol.ConnectionCommands;
import io.codis.nedis.protocol.HashesCommands;
import io.codis.nedis.protocol.HyperLogLogCommands;
import io.codis.nedis.protocol.KeysCommands;
import io.codis.nedis.protocol.ListsCommands;
import io.codis.nedis.protocol.ScriptingCommands;
import io.codis.nedis.protocol.ServerCommands;
import io.codis.nedis.protocol.SetsCommands;
import io.codis.nedis.protocol.SortedSetsCommands;
import io.codis.nedis.protocol.StringsCommands;
import io.codis.nedis.protocol.TransactionsCommands;
import io.netty.util.concurrent.Future;

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
