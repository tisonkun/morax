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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.exception.MoraxException;
import org.tisonkun.morax.rpc.IsolatedRpcEndpoint;
import org.tisonkun.morax.rpc.RpcEndpoint;
import org.tisonkun.morax.rpc.RpcEndpointAddress;
import org.tisonkun.morax.rpc.RpcEndpointRef;
import org.tisonkun.morax.rpc.exception.RpcEnvStoppedException;
import org.tisonkun.morax.rpc.network.client.RpcResponseCallback;
import org.tisonkun.morax.util.ThrowableUtils;

@Slf4j
public class Dispatcher {
    private final Object lock = new Object();

    /**
     * True if the dispatcher has been stopped. Once stopped, all messages posted will be bounced
     * immediately.
     */
    @GuardedBy("lock")
    private boolean stopped = false;

    private final ConcurrentMap<String, MessageLoop> endpoints = new ConcurrentHashMap<>();
    private final ConcurrentMap<RpcEndpoint, RpcEndpointRef> endpointRefs = new ConcurrentHashMap<>();

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    // TODO(@tison) lazy val
    private final SharedMessageLoop sharedLoop = new SharedMessageLoop(this);

    private final NettyRpcEnv nettyRpcEnv;

    public Dispatcher(NettyRpcEnv nettyRpcEnv) {
        this.nettyRpcEnv = nettyRpcEnv;
    }

    public NettyRpcEndpointRef registerRpcEndpoint(String name, RpcEndpoint endpoint) {
        final RpcEndpointAddress addr = new RpcEndpointAddress(nettyRpcEnv.address(), name);
        final NettyRpcEndpointRef endpointRef = new NettyRpcEndpointRef(addr, nettyRpcEnv);

        synchronized (lock) {
            Preconditions.checkState(!stopped, "RpcEnv has been stopped");
            Preconditions.checkState(!endpoints.containsKey(name), "There is already an RpcEndpoint called " + name);

            // This must be done before assigning RpcEndpoint to MessageLoop, as MessageLoop sets Inbox be
            // active when registering, and endpointRef must be put into endpointRefs before onStart is
            // called.
            endpointRefs.put(endpoint, endpointRef);

            try {
                final MessageLoop messageLoop;
                if (endpoint instanceof IsolatedRpcEndpoint e) {
                    messageLoop = new DedicatedMessageLoop(name, e, this);
                } else {
                    sharedLoop.register(name, endpoint);
                    messageLoop = sharedLoop;
                }
                endpoints.put(name, messageLoop);
            } catch (Throwable t) {
                if (ThrowableUtils.isNonFatal(t)) {
                    endpointRefs.remove(endpoint);
                }
                throw ThrowableUtils.sneakyThrow(t);
            }
        }

        return endpointRef;
    }

    public RpcEndpointRef getRpcEndpointRef(RpcEndpoint endpoint) {
        return endpointRefs.get(endpoint);
    }

    public void removeRpcEndpointRef(RpcEndpoint endpoint) {
        endpointRefs.remove(endpoint);
    }

    // Should be idempotent
    private void unregisterRpcEndpoint(String name) {
        final MessageLoop loop = endpoints.remove(name);
        if (loop != null) {
            loop.unregister(name);
        }
        // Don't clean `endpointRefs` here because it's possible that some messages are being processed
        // now, and they can use `getRpcEndpointRef`. So `endpointRefs` will be cleaned in Inbox via
        // `removeRpcEndpointRef`.
    }

    /**
     * Send a message to all registered {@link RpcEndpoint}s in this process.
     * <p>
     * This can be used to make network events known to all end points (e.g. "a new node connected").
     */
    public void postToAll(InboxMessage message) {
        final var it = endpoints.keySet().iterator();
        while (it.hasNext()) {
            final String name = it.next();
            postMessage(name, message, e -> {
                if (e instanceof RpcEnvStoppedException) {
                    log.debug("Message {} dropped.", message, e);
                } else {
                    log.warn("Message {} dropped.", message, e);
                }
            });
        }
    }

    /**
     * Posts a one-way message.
     */
    public void postOneWayMessage(RequestMessage message) {
        final String endpointName = message.receiver().name();
        final InboxMessage inboxMessage = new InboxMessage.OneWay(message.senderAddress(), message.content());
        postMessage(endpointName, inboxMessage, e -> {
            // SPARK-31922: in local cluster mode, there's always a RpcEnvStoppedException when
            // stop is called due to some asynchronous message handling. We catch the exception
            // and log it at debug level to avoid verbose error message when user stop a local
            // cluster in spark shell.
            if (e instanceof RpcEnvStoppedException) {
                log.debug("Message {} dropped.", message, e);
            } else {
                throw ThrowableUtils.sneakyThrow(e);
            }
        });
    }

    /**
     * Posts a message sent by a local endpoint.
     */
    public void postLocalMessage(RequestMessage message, CompletableFuture<Object> f) {
        final var rpcCallContext = new LocalNettyRpcCallContext(message.senderAddress(), f);
        final var rpcMessage = new InboxMessage.Rpc(message.senderAddress(), message.content(), rpcCallContext);
        postMessage(message.receiver().name(), rpcMessage, f::completeExceptionally);
    }

    /**
     * Posts a message sent by a remote endpoint.
     */
    public void postRemoteMessage(RequestMessage message, RpcResponseCallback callback) {
        final var rpcCallContext = new RemoteNettyRpcCallContext(message.senderAddress(), nettyRpcEnv, callback);
        final var rpcMessage = new InboxMessage.Rpc(message.senderAddress(), message.content(), rpcCallContext);
        postMessage(message.receiver().name(), rpcMessage, callback::onFailure);
    }

    /**
     * Posts a message to a specific endpoint.
     *
     * @param endpointName      name of the endpoint.
     * @param message           the message to post
     * @param callbackIfStopped callback function if the endpoint is stopped.
     */
    private void postMessage(String endpointName, InboxMessage message, Consumer<Exception> callbackIfStopped) {
        final Optional<Exception> exception;
        synchronized (lock) {
            final MessageLoop loop = endpoints.get(endpointName);
            if (stopped) {
                exception = Optional.of(new RpcEnvStoppedException());
            } else if (loop == null) {
                exception = Optional.of(new MoraxException("Could not find %s.".formatted(endpointName)));
            } else {
                loop.post(endpointName, message);
                exception = Optional.empty();
            }
        }
        // We don't need to call `onStop` in the `synchronized` block
        exception.ifPresent(callbackIfStopped);
    }

    public void stop(RpcEndpointRef ref) {
        synchronized (lock) {
            if (stopped) {
                return;
            }
        }
        unregisterRpcEndpoint(ref.name());
    }

    public void stop() {
        synchronized (lock) {
            if (stopped) {
                return;
            }
            stopped = true;
        }

        boolean stopSharedLoop = false;
        for (Map.Entry<String, MessageLoop> entry : endpoints.entrySet()) {
            final String name = entry.getKey();
            final MessageLoop loop = entry.getValue();
            unregisterRpcEndpoint(name);
            if (loop instanceof SharedMessageLoop) {
                stopSharedLoop = true;
            } else {
                loop.stop();
            }
        }

        if (stopSharedLoop) {
            sharedLoop.stop();
        }

        shutdownLatch.countDown();
    }

    public void awaitTermination() throws InterruptedException {
        shutdownLatch.await();
    }

    /**
     * Return if the endpoint exists
     */
    public boolean verify(String name) {
        return endpoints.containsKey(name);
    }
}
