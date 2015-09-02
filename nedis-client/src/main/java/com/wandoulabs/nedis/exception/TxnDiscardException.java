package com.wandoulabs.nedis.exception;

import java.io.IOException;

/**
 * @author Apache9
 */
public class TxnDiscardException extends IOException {

    private static final long serialVersionUID = -1088947518802949094L;

    public TxnDiscardException() {
        super();
    }

    public TxnDiscardException(String message, Throwable cause) {
        super(message, cause);
    }

    public TxnDiscardException(String message) {
        super(message);
    }

    public TxnDiscardException(Throwable cause) {
        super(cause);
    }

}
