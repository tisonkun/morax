package org.tisonkun.nymph.rpc.netty;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.tisonkun.nymph.rpc.RpcEndpoint;
import org.tisonkun.nymph.util.ThreadUtils;

/**
 * A message loop that serves multiple RPC endpoints, using a shared thread pool.
 */
public final class SharedMessageLoop extends MessageLoop {

    // TODO(@tison) respect NymphConfig
    private static int getNumOfThreads() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Thread pool used for dispatching messages.
     */
    private static ExecutorService executorService(int numThreads) {
        return ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop");
    }

    private final ConcurrentMap<String, Inbox> endpoints = new ConcurrentHashMap<>();

    public SharedMessageLoop(Dispatcher dispatcher) {
        super(dispatcher, executorService(getNumOfThreads()));

        final int numThreads = getNumOfThreads();
        for (int i = 0; i < numThreads; i++) {
            executorService.execute(receiveLoopRunnable);
        }
    }

    @Override
    public void post(String endpointName, InboxMessage message) {
        final Inbox inbox = endpoints.get(endpointName);
        inbox.post(message);
        setActive(inbox);
    }

    @Override
    public void unregister(String endpointName) {
        final Inbox inbox = endpoints.remove(endpointName);
        if (inbox != null) {
            inbox.stop();
            // Mark active to handle the OnStop message.
            setActive(inbox);
        }
    }

    public void register(String name, RpcEndpoint endpoint) {
        final Inbox inbox = new Inbox(name, endpoint);
        endpoints.put(name, inbox);
        // Mark active to handle the OnStart message.
        setActive(inbox);
    }
}
