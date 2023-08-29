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

import com.google.common.base.Preconditions;
import java.util.LinkedList;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.exception.MoraxException;
import org.tisonkun.morax.rpc.RpcEndpoint;
import org.tisonkun.morax.rpc.ThreadSafeRpcEndpoint;
import org.tisonkun.morax.util.ThrowableUtils;

/**
 * An inbox that stores messages for an {@link RpcEndpoint} and posts messages to it thread-safely.
 */
@Slf4j
public class Inbox {
    private final Object lock = new Object();

    @GuardedBy("lock")
    private final LinkedList<InboxMessage> messages = new LinkedList<>();

    /**
     * True if the inbox (and its associated endpoint) is stopped.
     */
    @GuardedBy("lock")
    private boolean stopped = false;

    /**
     * Allow multiple threads to process messages at the same time.
     */
    @GuardedBy("lock")
    private boolean enableConcurrent = false;

    /**
     * The number of threads processing messages for this inbox.
     */
    @GuardedBy("this")
    private int numActiveThreads = 0;

    private final String endpointName;
    private final RpcEndpoint endpoint;

    public Inbox(String endpointName, RpcEndpoint endpoint) {
        this.endpointName = endpointName;
        this.endpoint = endpoint;
        // OnStart should be the first message to process
        this.messages.add(new InboxMessage.OnStart());
    }

    /**
     * Process stored messages.
     */
    public void process(Dispatcher dispatcher) {
        InboxMessage polledMessage;
        synchronized (lock) {
            if (!enableConcurrent && numActiveThreads != 0) {
                return;
            }
            polledMessage = messages.poll();
            if (polledMessage != null) {
                numActiveThreads += 1;
            } else {
                return;
            }
        }

        while (true) {
            // NOTE: lambda expression only captures final local variables
            final InboxMessage message = polledMessage;
            safelyCall(endpoint, () -> {
                if (message instanceof InboxMessage.Rpc m) {
                    try {
                        if (!endpoint.receiveAndReply(m.content(), m.context())) {
                            throw new MoraxException(
                                    "Unsupported message %s from %s".formatted(message, m.remoteAddress()));
                        }
                    } catch (Throwable t) {
                        m.context().sendFailure(t);
                        // Throw the exception: this exception will be caught by the safelyCall function.
                        // The endpoint's onError function will be called.
                        throw t;
                    }
                } else if (message instanceof InboxMessage.OneWay m) {
                    if (!endpoint.receive(m.content())) {
                        throw new MoraxException(
                                "Unsupported message %s from %s".formatted(message, m.remoteAddress()));
                    }
                } else if (message instanceof InboxMessage.OnStart) {
                    endpoint.onStart();
                    if (!(endpoint instanceof ThreadSafeRpcEndpoint)) {
                        synchronized (lock) {
                            if (!stopped) {
                                enableConcurrent = true;
                            }
                        }
                    }
                } else if (message instanceof InboxMessage.OnStop) {
                    final int activeThreads = getNumActiveThreads();
                    Preconditions.checkState(
                            activeThreads == 1,
                            "There should be only a single active thread but found %s threads.",
                            activeThreads);
                    dispatcher.removeRpcEndpointRef(endpoint);
                    endpoint.onStop();
                    Preconditions.checkState(isEmpty(), "OnStop should be the last message");
                } else if (message instanceof InboxMessage.RemoteProcessConnected m) {
                    endpoint.onConnected(m.remoteAddress());
                } else if (message instanceof InboxMessage.RemoteProcessDisconnected m) {
                    endpoint.onDisconnected(m.remoteAddress());
                } else if (message instanceof InboxMessage.RemoteProcessConnectionError m) {
                    endpoint.onNetworkError(m.cause(), m.remoteAddress());
                } // unreachable due to sealed implementations - once upgrade to JDK 21 we can use switch case
            });

            synchronized (lock) {
                // "enableConcurrent" will be set to false after `onStop` is called, so we should check it
                // every time.

                if (!enableConcurrent && numActiveThreads != 1) {
                    // If we are not the only one worker, exit
                    numActiveThreads -= 1;
                    return;
                }

                polledMessage = messages.poll();
                if (message == null) {
                    numActiveThreads -= 1;
                    return;
                }
            }
        }
    }

    public void post(InboxMessage message) {
        synchronized (lock) {
            if (stopped) {
                // We already put "OnStop" into "messages", so we should drop further messages
                onDrop(message);
            } else {
                messages.add(message);
            }
        }
    }

    /**
     * Called when we are dropping a message. Test cases override this to test message dropping.
     * Exposed for testing.
     */
    protected void onDrop(InboxMessage message) {
        log.warn("Drop {} because endpoint {} is stopped", message, endpointName);
    }

    public void stop() {
        synchronized (lock) {
            // The following codes should be in `synchronized` so that we can make sure "OnStop" is the last
            // message
            if (!stopped) {
                // We should disable concurrent here. Then when RpcEndpoint.onStop is called, it's the only
                // thread that is processing messages. So `RpcEndpoint.onStop` can release its resources
                // safely.
                enableConcurrent = false;
                stopped = true;
                messages.add(new InboxMessage.OnStop());
                // Note: The concurrent events in messages will be processed one by one.
            }
        }
    }

    public boolean isEmpty() {
        synchronized (lock) {
            return messages.isEmpty();
        }
    }

    public int getNumActiveThreads() {
        synchronized (lock) {
            return this.numActiveThreads;
        }
    }

    /**
     * Calls action closure, and calls the endpoint's onError function in the case of exceptions.
     */
    private void safelyCall(RpcEndpoint endpoint, Runnable action) {
        try {
            action.run();
        } catch (Throwable throwable) {
            if (ThrowableUtils.isNonFatal(throwable)) {
                try {
                    endpoint.onError(throwable);
                } catch (Throwable t) {
                    if (ThrowableUtils.isNonFatal(t)) {
                        if (stopped) {
                            log.debug("Ignoring error (endpoint {} stopped).", endpointName, t);
                        } else {
                            log.error("Ignoring error (endpoint {} running).", endpointName, t);
                        }
                    } else {
                        dealWithFatalError(throwable);
                    }
                }
            } else {
                dealWithFatalError(throwable);
            }
        }
    }

    private void dealWithFatalError(Throwable fatal) {
        synchronized (lock) {
            Preconditions.checkState(numActiveThreads > 0, "The number of active threads should be positive.");
            // Should reduce the number of active threads before throw the error.
            numActiveThreads -= 1;
        }

        log.error("An error happened while processing message in the inbox for {}", endpointName, fatal);
        throw ThrowableUtils.sneakyThrow(fatal);
    }
}
