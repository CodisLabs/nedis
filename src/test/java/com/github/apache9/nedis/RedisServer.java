package com.github.apache9.nedis;

import java.io.IOException;

/**
 * @author Apache9
 */
public class RedisServer {

    private final ProcessBuilder builder;

    private Process process;

    public RedisServer(int port) {
        builder = new ProcessBuilder().command("redis-server", "--port", Long.toString(port))
                .inheritIO();
    }

    public void start() throws IOException {
        process = builder.start();
    }

    public void stop() {
        if (process != null) {
            process.destroy();
        }
    }
}
