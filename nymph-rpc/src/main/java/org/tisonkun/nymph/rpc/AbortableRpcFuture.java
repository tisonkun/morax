package org.tisonkun.nymph.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A subclass of {@link CompletableFuture} that adds abort method.
 * This is used in long run RPC and provide an approach to abort the RPC.
 */
public class AbortableRpcFuture<T> extends CompletableFuture<T> {
    private final Consumer<Throwable> onAbort;

    public AbortableRpcFuture(Consumer<Throwable> onAbort) {
        this.onAbort = onAbort;
    }

    public void abort(Throwable t) {
        onAbort.accept(t);
    }
}
