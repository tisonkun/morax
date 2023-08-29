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
import java.util.concurrent.Future;
import javax.annotation.concurrent.GuardedBy;
import lombok.Getter;
import org.tisonkun.morax.exception.MoraxException;
import org.tisonkun.morax.rpc.RpcAddress;
import org.tisonkun.morax.rpc.network.client.TransportClient;
import org.tisonkun.morax.util.ThrowableUtils;

public class Outbox {
    private final Object lock = new Object();

    @GuardedBy("lock")
    private final LinkedList<OutboxMessage> messages = new LinkedList<>();

    @GuardedBy("lock")
    private TransportClient client;

    /**
     * connectFuture points to the connect task. If there is no connect task, connectFuture will be
     * null.
     */
    @GuardedBy("lock")
    private Future<?> connectFuture;

    @GuardedBy("lock")
    private boolean stopped = false;

    /**
     * If there is any thread draining the message queue
     */
    @GuardedBy("this")
    private boolean draining = false;

    private final NettyRpcEnv nettyRpcEnv;

    @Getter
    private final RpcAddress address;

    public Outbox(NettyRpcEnv nettyRpcEnv, RpcAddress address) {
        this.nettyRpcEnv = nettyRpcEnv;
        this.address = address;
    }

    /**
     * Send a message. If there is no active connection, cache it and launch a new connection. If
     * {@link Outbox} is stopped, the sender will be notified with a {@link MoraxException}.
     */
    public void send(OutboxMessage message) {
        final boolean dropped;
        synchronized (lock) {
            if (stopped) {
                dropped = true;
            } else {
                messages.add(message);
                dropped = false;
            }
        }

        if (dropped) {
            message.onFailure(new MoraxException("Message is dropped because Outbox is stopped"));
        } else {
            drainOutbox();
        }
    }

    /**
     * Drain the message queue. If there is other draining thread, just exit. If the connection has
     * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to set up the
     * connection.
     */
    private void drainOutbox() {
        OutboxMessage message;

        synchronized (lock) {
            if (stopped) {
                return;
            }
            if (connectFuture != null) {
                // We are connecting to the remote address, so just exit
                return;
            }
            if (client == null) {
                // There is no connect task but client is null, so we need to launch the connect task.
                launchConnectTask();
                return;
            }
            if (draining) {
                // There is some thread draining, so just exit
                return;
            }
            message = messages.poll();
            if (message == null) {
                return;
            }
            draining = true;
        }

        while (true) {
            try {
                final TransportClient client;
                synchronized (lock) {
                    client = this.client;
                }
                if (client != null) {
                    message.sendWith(client);
                } else {
                    Preconditions.checkState(stopped);
                }
            } catch (Throwable t) {
                if (ThrowableUtils.isNonFatal(t)) {
                    handleNetworkFailure(t);
                    return;
                } else {
                    throw ThrowableUtils.sneakyThrow(t);
                }
            }

            synchronized (lock) {
                if (stopped) {
                    return;
                }
                message = messages.poll();
                if (message == null) {
                    draining = false;
                    return;
                }
            }
        }
    }

    private void launchConnectTask() {
        connectFuture = nettyRpcEnv.getClientConnectionExecutor().submit(() -> {
            try {
                final TransportClient client = nettyRpcEnv.createClient(address);
                synchronized (lock) {
                    this.client = client;
                    if (stopped) {
                        closeClient();
                    }
                }
            } catch (InterruptedException e) {
                // exit
                return;
            } catch (Throwable t) {
                if (ThrowableUtils.isNonFatal(t)) {
                    synchronized (lock) {
                        this.connectFuture = null;
                    }
                    handleNetworkFailure(t);
                    return;
                } else {
                    throw ThrowableUtils.sneakyThrow(t);
                }
            }

            synchronized (lock) {
                this.connectFuture = null;
            }
            // It's possible that no thread is draining now. If we don't drain here, we cannot send the
            // messages until the next message arrives.
            drainOutbox();
        });
    }

    /**
     * Stop {@link Inbox} and notify the waiting messages with the cause.
     */
    private void handleNetworkFailure(Throwable t) {
        synchronized (lock) {
            Preconditions.checkState(connectFuture == null);
            if (stopped) {
                return;
            }
            stopped = true;
            closeClient();
        }

        // Remove this Outbox from nettyEnv so that the further messages will create a new Outbox along
        // with a new connection
        nettyRpcEnv.removeOutbox(address);

        // Notify the connection failure for the remaining messages
        //
        // We always check `stopped` before updating messages, so here we can make sure no thread will
        // update messages, and it's safe to just drain the queue.
        var message = messages.poll();
        while (message != null) {
            message.onFailure(t);
            message = messages.poll();
        }
        Preconditions.checkState(messages.isEmpty());
    }

    /**
     * Stop {@link Outbox}. The remaining messages in the {@link Outbox} will be notified with a
     * {@link MoraxException}.
     */
    public void stop() {
        synchronized (lock) {
            if (stopped) {
                return;
            }
            stopped = true;
            if (connectFuture != null) {
                connectFuture.cancel(true);
            }
            closeClient();
        }

        // We always check `stopped` before updating messages, so here we can make sure no thread will
        // update messages, and it's safe to just drain the queue.
        var message = messages.poll();
        while (message != null) {
            message.onFailure(new MoraxException("Message is dropped because Outbox is stopped"));
            message = messages.poll();
        }
    }

    private void closeClient() {
        synchronized (lock) {
            // Just set client to null. Don't close it in order to reuse the connection.
            client = null;
        }
    }
}
