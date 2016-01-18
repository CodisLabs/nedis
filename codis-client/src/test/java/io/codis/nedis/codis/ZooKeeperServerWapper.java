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
package io.codis.nedis.codis;

import java.io.File;
import java.io.IOException;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

/**
 * @author Apache9
 */
public class ZooKeeperServerWapper {

    private volatile ServerCnxnFactory cnxnFactory;

    private volatile ZooKeeperServer zkServer;

    private volatile FileTxnSnapLog ftxn;

    private int port;

    private File baseDir;

    public ZooKeeperServerWapper(int port, File baseDir) throws Exception {
        this.port = port;
        this.baseDir = baseDir;
    }

    public void start() throws IOException, InterruptedException {
        ZooKeeperServer zkServer = new ZooKeeperServer();
        FileTxnSnapLog ftxn = new FileTxnSnapLog(
                new File(baseDir, "zookeeper"), new File(baseDir, "zookeeper"));
        zkServer.setTxnLogFactory(ftxn);
        zkServer.setTickTime(1000);
        ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory(port,
                100);
        cnxnFactory.startup(zkServer);
        this.cnxnFactory = cnxnFactory;
        this.zkServer = zkServer;
        this.ftxn = ftxn;
    }

    public void stop() throws IOException {
        cnxnFactory.shutdown();
        ftxn.close();
    }

    public boolean isRunning() {
        if (zkServer == null) {
            return false;
        } else {
            return zkServer.isRunning();
        }
    }
}
