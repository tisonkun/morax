package org.tisonkun.nymph.rpc.netty;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.tisonkun.nymph.rpc.RpcEndpoint;
import org.tisonkun.nymph.rpc.RpcEndpointRef;

public class Dispatcher {
    private final ConcurrentMap<RpcEndpoint, RpcEndpointRef> endpointRefs = new ConcurrentHashMap<>();

    public RpcEndpointRef getRpcEndpointRef(RpcEndpoint endpoint) {
        return endpointRefs.get(endpoint);
    }

    public void removeRpcEndpointRef(RpcEndpoint endpoint) {
        endpointRefs.remove(endpoint);
    }
}
