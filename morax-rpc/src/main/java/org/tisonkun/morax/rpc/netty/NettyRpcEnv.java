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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.exception.MoraxException;
import org.tisonkun.morax.io.ByteBufferInputStream;
import org.tisonkun.morax.rpc.AbortableRpcFuture;
import org.tisonkun.morax.rpc.RpcAddress;
import org.tisonkun.morax.rpc.RpcEndpoint;
import org.tisonkun.morax.rpc.RpcEndpointAddress;
import org.tisonkun.morax.rpc.RpcEndpointRef;
import org.tisonkun.morax.rpc.RpcEnv;
import org.tisonkun.morax.rpc.RpcEnvConfig;
import org.tisonkun.morax.rpc.RpcTimeout;
import org.tisonkun.morax.rpc.exception.RpcEndpointNotFoundException;
import org.tisonkun.morax.rpc.exception.RpcEnvStoppedException;
import org.tisonkun.morax.rpc.network.TransportContext;
import org.tisonkun.morax.rpc.network.client.TransportClient;
import org.tisonkun.morax.rpc.network.client.TransportClientFactory;
import org.tisonkun.morax.rpc.network.config.MapConfigProvider;
import org.tisonkun.morax.rpc.network.config.TransportConfig;
import org.tisonkun.morax.rpc.network.server.TransportServer;
import org.tisonkun.morax.util.DynamicVariable;
import org.tisonkun.morax.util.ThreadUtils;
import org.tisonkun.morax.util.ThrowableUtils;

@Slf4j
public class NettyRpcEnv extends RpcEnv {
    /**
     * When deserializing the {@link NettyRpcEndpointRef}, it needs a reference to {@link NettyRpcEnv}.
     * Use CURRENT_ENV to wrap the deserialization codes.
     */
    static final DynamicVariable<NettyRpcEnv> CURRENT_ENV = new DynamicVariable<>(null);

    /**
     * Similar to CURRENT_ENV, this variable references the client instance associated with an
     * RPC, in case it's needed to find out the remote address during deserialization.
     */
    static final DynamicVariable<TransportClient> CURRENT_CLIENT = new DynamicVariable<>(null);

    // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
    // to implement non-blocking send/ask.
    @Getter(AccessLevel.MODULE)
    private final ExecutorService clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool(
            "netty-rpc-connection",
            // TODO(@tison) respect MoraxConfig
            64);

    private final ScheduledExecutorService timeoutScheduler =
            ThreadUtils.newDaemonSingleThreadScheduledExecutor("netty-rpc-env-timeout");

    private final Dispatcher dispatcher = new Dispatcher(this);

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    // TODO(@tison) configurable
    private final TransportConfig transportConfig =
            new TransportConfig("rpc", new MapConfigProvider(Collections.emptyMap()));

    private final TransportContext transportContext =
            new TransportContext(transportConfig, new NettyRpcHandler(dispatcher, this));

    private final TransportClientFactory clientFactory = transportContext.createClientFactory();

    /**
     * A map for {@link RpcAddress} and {@link Outbox}. When we are connecting to a remote {@link RpcAddress},
     * we just put messages to its {@link Outbox} to implement a non-blocking {@link #send} method.
     */
    private final ConcurrentMap<RpcAddress, Outbox> outboxes = new ConcurrentHashMap<>();

    private final TransportServer server;
    private final String host;

    public NettyRpcEnv(RpcEnvConfig config) {
        this.host = config.advertiseAddress();
        this.server = transportContext.createServer(config.bindAddress(), config.port());
        dispatcher.registerRpcEndpoint(RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher));
    }

    @Override
    public RpcEndpointRef endpointRef(RpcEndpoint endpoint) {
        return dispatcher.getRpcEndpointRef(endpoint);
    }

    @Override
    public RpcAddress address() {
        if (server != null) {
            return new RpcAddress(host, server.getPort());
        } else {
            return null;
        }
    }

    @Override
    public RpcEndpointRef setupEndpoint(String name, RpcEndpoint endpoint) {
        return dispatcher.registerRpcEndpoint(name, endpoint);
    }

    @Override
    public CompletableFuture<RpcEndpointRef> asyncSetupEndpointRefByURI(String uri) {
        final RpcEndpointAddress addr = RpcEndpointAddress.create(uri);
        final NettyRpcEndpointRef endpointRef = new NettyRpcEndpointRef(addr, this);
        final NettyRpcEndpointRef verifier =
                new NettyRpcEndpointRef(new RpcEndpointAddress(addr.rpcAddress(), RpcEndpointVerifier.NAME), this);
        final CompletableFuture<Boolean> f = verifier.ask(new RpcEndpointVerifier.CheckExistence(endpointRef.name()));
        return f.thenCompose(find -> {
            if (find) {
                return CompletableFuture.completedFuture(endpointRef);
            } else {
                return CompletableFuture.failedFuture(new RpcEndpointNotFoundException(uri));
            }
        });
    }

    @Override
    public void stop(RpcEndpointRef endpointRef) {
        Preconditions.checkArgument(endpointRef instanceof NettyRpcEndpointRef);
        dispatcher.stop(endpointRef);
    }

    @Override
    public void shutdown() {
        cleanup();
    }

    private void cleanup() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        for (Outbox outbox : outboxes.values()) {
            outboxes.remove(outbox.getAddress());
            outbox.stop();
        }

        timeoutScheduler.shutdownNow();
        dispatcher.stop();
        server.close();
        clientFactory.close();
        clientConnectionExecutor.shutdownNow();
        transportContext.close();
    }

    @Override
    public void awaitTermination() {
        try {
            dispatcher.awaitTermination();
        } catch (InterruptedException e) {
            throw ThrowableUtils.sneakyThrow(e);
        }
    }

    @Override
    public <T> T deserialize(Supplier<T> deserializationAction) {
        return null;
    }

    @SuppressWarnings("unchecked")
    private <T> T deserialize(TransportClient client, ByteBuffer bytes) {
        return (T) NettyRpcEnv.CURRENT_CLIENT.withValue(client, () -> {
            try (final ByteBufferInputStream bis = new ByteBufferInputStream(bytes);
                    final ObjectInputStream ois = new ObjectInputStream(bis)) {
                return ois.readObject();
            }
        });
    }

    TransportClient createClient(RpcAddress address) throws IOException, InterruptedException {
        return clientFactory.createClient(address.host(), address.port());
    }

    /**
     * Remove the address's Outbox and stop it.
     */
    void removeOutbox(RpcAddress address) {
        final Outbox outbox = outboxes.remove(address);
        if (outbox != null) {
            outbox.stop();
        }
    }

    // TODO(@tison) replace with more generic and effective serialization.
    @SneakyThrows
    ByteBuffer serialize(Object content) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(content);
        oos.close();
        return ByteBuffer.wrap(bos.toByteArray());
    }

    void send(RequestMessage message) {
        final RpcAddress remoteAddr = message.receiver().address();
        if (remoteAddr.equals(address())) {
            // Message to a local RPC endpoint.
            dispatcher.postOneWayMessage(message);
        } else {
            // Message to a remote RPC endpoint.
            postToOutbox(message.receiver(), new OutboxMessage.OneWay(message.serialize()));
        }
    }

    <T> AbortableRpcFuture<T> askAbortable(RequestMessage message, RpcTimeout timeout) {
        final CompletableFuture<Object> future = new CompletableFuture<>();
        final RpcAddress remoteAddr = message.receiver().address();

        final AtomicReference<OutboxMessage.Rpc> rpcMsg = new AtomicReference<>();

        final Consumer<Throwable> onFailure = e -> {
            if (!future.completeExceptionally(e)) {
                if (e instanceof RpcEnvStoppedException) {
                    log.debug("Ignored failure.", e);
                } else {
                    log.warn("Ignored failure.", e);
                }
            }
        };

        final Consumer<Object> onSuccess = reply -> {
            if (reply instanceof RpcFailure failure) {
                onFailure.accept(failure.t());
            } else {
                if (!future.complete(reply)) {
                    log.warn("Ignored message: {}", reply);
                }
            }
        };

        final Consumer<Throwable> onAbort = e -> {
            onFailure.accept(e);
            final OutboxMessage.Rpc rpc = rpcMsg.get();
            if (rpc != null) {
                rpc.onAbort();
            }
        };

        try {
            if (remoteAddr.equals(address())) {
                final CompletableFuture<Object> f = new CompletableFuture<>();
                f.whenComplete((r, t) -> {
                    if (r != null) {
                        onSuccess.accept(r);
                    } else {
                        onFailure.accept(t);
                    }
                });
                dispatcher.postLocalMessage(message, f);
            } else {
                final OutboxMessage.Rpc rpcMessage = new OutboxMessage.Rpc(
                        message.serialize(), (client, resp) -> onSuccess.accept(deserialize(client, resp)), onFailure);
                rpcMsg.set(rpcMessage);
                postToOutbox(message.receiver(), rpcMessage);
                future.exceptionally(e -> {
                    if (e instanceof TimeoutException) {
                        rpcMessage.onTimeout();
                    }
                    return null;
                });
            }

            final ScheduledFuture<?> timeoutCancelable = timeoutScheduler.schedule(
                    () -> onFailure.accept(new MoraxException(
                            "Cannot receive any reply from " + remoteAddr + " in " + timeout.duration())),
                    timeout.duration().toNanos(),
                    TimeUnit.NANOSECONDS);
            future.whenComplete((i1, i2) -> timeoutCancelable.cancel(true));
        } catch (Throwable t) {
            if (ThrowableUtils.isNonFatal(t)) {
                onFailure.accept(t);
            } else {
                throw ThrowableUtils.sneakyThrow(t);
            }
        }

        final CompletableFuture<T> f = future.thenApply(result -> {
                    //noinspection unchecked
                    return (T) result;
                })
                .exceptionally(timeout::addMessageIfTimeout);
        return new AbortableRpcFuture<>(f, onAbort);
    }

    private void postToOutbox(NettyRpcEndpointRef receiver, OutboxMessage message) {
        if (receiver.getClient() != null) {
            message.sendWith(receiver.getClient());
        } else {
            Preconditions.checkNotNull(
                    receiver.address(), "Cannot send message to client endpoint with no listen address.");
            final Outbox targetOutbox;
            final Outbox outbox = outboxes.get(receiver.address());
            if (outbox != null) {
                targetOutbox = outbox;
            } else {
                final Outbox newOutbox = new Outbox(this, receiver.address());
                final Outbox oldOutbox = outboxes.putIfAbsent(receiver.address(), newOutbox);
                targetOutbox = Objects.requireNonNullElse(oldOutbox, newOutbox);
            }
            if (stopped.get()) {
                // It's possible that we put `targetOutbox` after stopping. So we need to clean it.
                outboxes.remove(receiver.address());
                targetOutbox.stop();
            } else {
                targetOutbox.send(message);
            }
        }
    }
}
