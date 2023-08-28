package org.tisonkun.nymph.rpc;

/**
 * An endpoint that uses a dedicated thread pool for delivering messages.
 */
public interface IsolatedRpcEndpoint extends RpcEndpoint {
    /**
     * How many threads to use for delivering messages.
     * <p>
     * Note that requesting more than one thread means that the endpoint should be able to handle
     * messages arriving from many threads at once, and all the things that entails (including
     * messages being delivered to the endpoint out of order).
     */
    int threadCount();
}
