package org.tisonkun.nymph.rpc;

/**
 * An endpoint that uses a dedicated thread pool for delivering messages and
 * ensured to be thread-safe.
 */
public interface IsolatedThreadSafeRpcEndpoint extends IsolatedRpcEndpoint {

    /**
     * Limit the threadCount to 1 so that messages are ensured to be handled in a thread-safe way.
     */
    default int threadCount() {
        return 1;
    }
}
