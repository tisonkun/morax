package org.tisonkun.nymph.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.Getter;

/**
 * A subclass of {@link CompletableFuture} that adds abort method.
 * This is used in long run RPC and provide an approach to abort the RPC.
 */
public class AbortableRpcFuture<T> {
    @Getter
    private final CompletableFuture<T> future;

    private final Consumer<Throwable> onAbort;

    public AbortableRpcFuture(CompletableFuture<T> future, Consumer<Throwable> onAbort) {
        this.future = future;
        this.onAbort = onAbort;
    }

    public void abort(Throwable t) {
        onAbort.accept(t);
    }
}
