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
