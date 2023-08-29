package org.tisonkun.nymph.rpc;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public abstract class RpcEndpointRef {
    private final RpcTimeout defaultAskTimeout = new RpcTimeout(
            // TODO(@tison) respect NymphConfig
            Duration.ofSeconds(5),
            "nymph.rpc.askTimeout");

    /**
     * Return the address for this.
     */
    public abstract RpcAddress address();

    public abstract String name();

    /**
     * Sends a one-way asynchronous message. Fire-and-forget semantics.
     */
    public abstract void send(Object message);

    public abstract <T> AbortableRpcFuture<T> askAbortable(Object message, RpcTimeout timeout);

    public abstract <T> CompletableFuture<T> ask(Object message, RpcTimeout timeout);

    public final <T> CompletableFuture<T> ask(Object message) {
        return ask(message, defaultAskTimeout);
    }

    public final <T> T askSync(Object message, RpcTimeout timeout) {
        return timeout.awaitResult(ask(message, timeout));
    }

    public final <T> T askSync(Object message) {
        return askSync(message, defaultAskTimeout);
    }
}
