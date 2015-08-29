package com.github.apache9.nedis;

import io.netty.util.concurrent.Future;

import com.github.apache9.nedis.protocol.ConnectionCommands;
import com.github.apache9.nedis.protocol.KeysCommands;
import com.github.apache9.nedis.protocol.ListsCommands;
import com.github.apache9.nedis.protocol.ScriptingCommands;
import com.github.apache9.nedis.protocol.StringsCommands;

/**
 * @author Apache9
 */
public interface NedisClient extends ConnectionManagement, ConnectionCommands, KeysCommands,
        StringsCommands, ScriptingCommands, ListsCommands {

    Future<Object> execCmd(byte[] cmd, byte[]... params);
}
