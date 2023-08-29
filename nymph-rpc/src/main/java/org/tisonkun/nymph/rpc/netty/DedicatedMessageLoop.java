package org.tisonkun.nymph.rpc.netty;

import com.google.common.base.Preconditions;
import java.util.concurrent.ExecutorService;
import org.tisonkun.nymph.rpc.IsolatedRpcEndpoint;
import org.tisonkun.nymph.util.ThreadUtils;

public final class DedicatedMessageLoop extends MessageLoop {
    private static ExecutorService executorService(String name, IsolatedRpcEndpoint endpoint) {
        if (endpoint.threadCount() > 1) {
            return ThreadUtils.newDaemonCachedThreadPool("dispatcher-" + name, endpoint.threadCount());
        } else {
            return ThreadUtils.newDaemonSingleThreadExecutor("dispatcher-" + name);
        }
    }

    private final String name;
    private final Inbox inbox;

    public DedicatedMessageLoop(String name, IsolatedRpcEndpoint endpoint, Dispatcher dispatcher) {
        super(dispatcher, executorService(name, endpoint));
        this.name = name;
        this.inbox = new Inbox(name, endpoint);

        for (int i = 0; i < endpoint.threadCount(); i++) {
            // We need to be careful not to use ExecutorService#submit, because
            // `submit` api will swallow uncaught exceptions in FutureTask#setException.
            executorService.execute(receiveLoopRunnable);
        }

        // Mark active to handle the OnStart message.
        setActive(inbox);
    }

    @Override
    public void post(String endpointName, InboxMessage message) {
        Preconditions.checkState(name.equals(endpointName), "name=%s but endpointName=%", name, endpointName);
        inbox.post(message);
        setActive(inbox);
    }

    @Override
    public void unregister(String endpointName) {
        synchronized (lock) {
            Preconditions.checkState(name.equals(endpointName), "name=%s but endpointName=%", name, endpointName);
            inbox.stop();
            // Mark active to handle the OnStop message.
            setActive(inbox);
            setActive(MessageLoop.POISON_PILL);
            executorService.shutdown();
        }
    }
}
