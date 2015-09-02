
package com.wandoulabs.nedis.exception;

import java.io.IOException;

/**
 *
 * @author Apache9
 *
 */
public class TxnAbortException extends IOException {

    private static final long serialVersionUID = -4546827177372832280L;

    public TxnAbortException() {
        super();
    }

    public TxnAbortException(String message, Throwable cause) {
        super(message, cause);
    }

    public TxnAbortException(String message) {
        super(message);
    }

    public TxnAbortException(Throwable cause) {
        super(cause);
    }

}
