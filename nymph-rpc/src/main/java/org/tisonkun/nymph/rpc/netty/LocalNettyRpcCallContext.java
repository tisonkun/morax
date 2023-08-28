package org.tisonkun.nymph.rpc.netty;

import java.util.concurrent.CompletableFuture;
import org.tisonkun.nymph.rpc.RpcAddress;

/**
 * If the sender and the receiver are in the same process, the reply can be sent back via local future.
 */
public class LocalNettyRpcCallContext extends NettyRpcCallContext {
    private final CompletableFuture<Object> future;

    public LocalNettyRpcCallContext(RpcAddress senderAddress, CompletableFuture<Object> future) {
        super(senderAddress);
        this.future = future;
    }

    @Override
    protected void send(Object message) {
        future.complete(message);
    }
}
