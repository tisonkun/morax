package org.tisonkun.nymph.rpc.netty;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tisonkun.nymph.util.ThrowableUtils;

public abstract sealed class MessageLoop permits DedicatedMessageLoop, SharedMessageLoop {
    /**
     * A poison inbox that indicates the message loop should stop processing messages.
     */
    protected static final Inbox POISON_PILL = new Inbox(null, null);

    private final Logger log = LoggerFactory.getLogger(getClass());

    protected final Object lock = new Object();

    // Message loop task; should be run in all threads of the message loop's pool.
    protected final Runnable receiveLoopRunnable = this::receiveLoop;

    // List of inboxes with pending messages, to be processed by the message loop.
    private final LinkedBlockingQueue<Inbox> active = new LinkedBlockingQueue<>();

    private final Dispatcher dispatcher;

    protected final ExecutorService executorService;

    @GuardedBy("lock")
    private boolean stopped = false;

    public MessageLoop(Dispatcher dispatcher, ExecutorService executorService) {
        this.dispatcher = dispatcher;
        this.executorService = executorService;
    }

    public abstract void post(String endpointName, InboxMessage message);

    public abstract void unregister(String endpointName);

    public final void stop() {
        synchronized (lock) {
            if (!stopped) {
                setActive(MessageLoop.POISON_PILL);
                executorService.shutdown();
                stopped = true;
            }
        }

        try {
            //noinspection ResultOfMethodCallIgnored
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw ThrowableUtils.sneakyThrow(e);
        }
    }

    protected final void setActive(Inbox inbox) {
        active.offer(inbox);
    }

    private void receiveLoop() {
        try {
            while (true) {
                try {
                    final Inbox inbox = active.take();
                    if (inbox == MessageLoop.POISON_PILL) {
                        // Put PoisonPill back so that other threads can see it.
                        setActive(MessageLoop.POISON_PILL);
                        return;
                    }
                    inbox.process(dispatcher);
                } catch (InterruptedException t) {
                    // exit
                    return;
                } catch (Throwable t) {
                    if (ThrowableUtils.isNonFatal(t)) {
                        log.error(t.getMessage(), t);
                    } else {
                        throw ThrowableUtils.sneakyThrow(t);
                    }
                }
            }
        } catch (Throwable t) {
            //noinspection finally
            try {
                // Re-submit a "receive" task so that message delivery will still work if
                // UncaughtExceptionHandler decides to not kill JVM.
                executorService.execute(receiveLoopRunnable);
            } finally {
                throw t;
            }
        }
    }
}
