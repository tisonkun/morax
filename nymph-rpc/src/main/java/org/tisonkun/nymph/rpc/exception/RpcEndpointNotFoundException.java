package org.tisonkun.nymph.rpc.exception;

import org.tisonkun.nymph.exception.NymphException;

public class RpcEndpointNotFoundException extends NymphException {
    public RpcEndpointNotFoundException(String uri) {
        super("Cannot find endpoint:" + uri);
    }
}
