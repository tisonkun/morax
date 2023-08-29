/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tisonkun.morax.rpc.netty;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tisonkun.morax.util.ThrowableUtils;

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
                executorService.shutdownNow();
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
