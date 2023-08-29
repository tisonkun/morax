package org.tisonkun.nymph.rpc;

import com.google.common.base.Preconditions;
import org.tisonkun.nymph.exception.NymphException;

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 * <p>
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 * <p>
 * The life-cycle of an endpoint is:
 * <p>
 * {@code constructor -> onStart -> receive* -> onStop}
 * <p>
 * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 * <p>
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
 */
public interface RpcEndpoint {
    /**
     * The {@link RpcEnv} that this {@link RpcEndpoint} is registered to.
     */
    RpcEnv rpcEnv();

    /**
     * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
     * called. And `self` will become `null` when `onStop` is called.
     *
     * <p>
     * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is no
     * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
     */
    default RpcEndpointRef self() {
        Preconditions.checkNotNull(rpcEnv(), "rpcEnv has not been initialized");
        return rpcEnv().endpointRef(this);
    }

    /**
     * Process messages from `RpcEndpointRef.send` or `RpcCallContext.reply`. If receiving an
     * unmatched message, `NymphException` will be thrown and sent to `onError`.
     *
     * @return whether the message is supported
     */
    default boolean receive(Object message) {
        throw new NymphException(self() + " does not implement 'receive'");
    }

    /**
     * Process messages from `RpcEndpointRef.ask`. If receiving an unmatched message,
     * `NymphException` will be thrown and sent to `onError`.
     *
     * @return whether the message is supported
     */
    default boolean receiveAndReply(Object message, RpcCallContext context) {
        context.sendFailure(new NymphException(self() + " won't reply anything"));
        return true;
    }

    /**
     * Invoked when any exception is thrown during handling messages.
     */
    default void onError(Throwable cause) throws Throwable {
        // By default, throw e and let RpcEnv handle it
        throw cause;
    }

    /**
     * Invoked when `remoteAddress` is connected to the current node.
     */
    default void onConnected(RpcAddress remoteAddress) {
        // By default, do nothing.
    }

    /**
     * Invoked when `remoteAddress` is lost.
     */
    default void onDisconnected(RpcAddress remoteAddress) {
        // By default, do nothing.
    }

    /**
     * Invoked when some network error happens in the connection between the current node and
     * `remoteAddress`.
     */
    default void onNetworkError(Throwable cause, RpcAddress remoteAddress) {
        // By default, do nothing.
    }

    /**
     * Invoked before [[RpcEndpoint]] starts to handle any message.
     */
    default void onStart() {
        // By default, do nothing.
    }

    /**
     * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method, and you cannot
     * use it to send or ask messages.
     */
    default void onStop() {
        // By default, do nothing.
    }

    /**
     * A convenient method to stop [[RpcEndpoint]].
     */
    default void stop() {
        final RpcEndpointRef self = self();
        if (self != null) {
            rpcEnv().stop(self);
        }
    }
}
