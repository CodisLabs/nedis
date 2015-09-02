package com.github.apache9.nedis.exception;

import java.io.IOException;

/**
 * @author Apache9
 */
public class RedisResponseException extends IOException {

    private static final long serialVersionUID = 1208471190036181159L;

    public RedisResponseException() {
        super();
    }

    public RedisResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisResponseException(String message) {
        super(message);
    }

    public RedisResponseException(Throwable cause) {
        super(cause);
    }

}
