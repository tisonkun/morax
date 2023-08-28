package org.tisonkun.nymph.rpc.exception;

import java.util.concurrent.TimeoutException;

public class RpcTimeoutException extends TimeoutException {
    public RpcTimeoutException(String message, TimeoutException cause) {
        super(message);
        initCause(cause);
    }
}
