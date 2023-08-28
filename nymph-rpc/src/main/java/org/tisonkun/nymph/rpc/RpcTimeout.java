package org.tisonkun.nymph.rpc;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.tisonkun.nymph.rpc.exception.RpcTimeoutException;
import org.tisonkun.nymph.util.ThrowableUtils;

/**
 * Associates a timeout with a description so that a when a TimeoutException occurs, additional
 * context about the timeout can be amended to the exception message.
 *
 * @param duration    timeout duration in seconds
 * @param timeoutProp the configuration property that controls this timeout
 */
public record RpcTimeout(Duration duration, String timeoutProp) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Amends the standard message of TimeoutException to include the description
     */
    private RpcTimeoutException createRpcTimeoutException(TimeoutException te) {
        return new RpcTimeoutException(te.getMessage() + ". This timeout is controlled by " + timeoutProp, te);
    }

    /**
     * Match a TimeoutException and add the timeout description to the message.
     * <p>
     * This can be used in the recover callback of a Future to add to a TimeoutException.
     */
    @SneakyThrows
    public <T> T addMessageIfTimeout(Throwable t) {
        if (t instanceof RpcTimeoutException te) {
            throw te;
        } else if (t instanceof TimeoutException te) {
            throw createRpcTimeoutException(te);
        } else {
            throw t;
        }
    }

    /**
     * Wait for the completed result and return it. If the result is not available within this
     * timeout, throw a {@link RpcTimeoutException} to indicate which configuration controls the timeout.
     *
     * @param future the Future to be awaited
     */
    @SneakyThrows
    public <T> T awaitResult(CompletableFuture<T> future) {
        try {
            return future.orTimeout(duration.toMillis(), TimeUnit.MILLISECONDS)
                    .exceptionally(this::addMessageIfTimeout)
                    .join();
        } catch (CompletionException e) {
            throw ThrowableUtils.stripCompletionException(e);
        }
    }
}
