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

package org.tisonkun.morax.rpc;

import com.google.common.base.Preconditions;
import org.tisonkun.morax.exception.MoraxException;

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 * <p>
 * It is guaranteed that {@link #onStart}, {@link #receive} and {@link #onStop} will be called in sequence.
 * <p>
 * The life-cycle of an endpoint is:
 * <p>
 * {@code constructor -> onStart -> receive* -> onStop}
 * <p>
 * Note: {@link #receive} can be called concurrently. If you want {@link #receive} to be thread-safe, please use
 * {@link ThreadSafeRpcEndpoint}
 * <p>
 * If any error is thrown from one of {@link RpcEndpoint} methods except {@link #onError}, {@link #onError} will be
 * invoked with the cause. If {@link #onError} throws an error, {@link RpcEnv} will ignore it.
 */
public interface RpcEndpoint {
    /**
     * The {@link RpcEnv} that this {@link RpcEndpoint} is registered to.
     */
    RpcEnv rpcEnv();

    /**
     * The {@link RpcEndpointRef} of this {@link RpcEndpoint}. {@code self} will become valid when {@link #onStart} is
     * called. And {@code self} will become {@code null} when {@link #onStop} is called.
     *
     * <p>
     * Note: Because before {@link #onStart}, {@link RpcEndpoint} has not yet been registered and there is no
     * valid {@link RpcEndpointRef} for it. So don't call {@code self} before {@link #onStart} is called.
     */
    default RpcEndpointRef self() {
        Preconditions.checkNotNull(rpcEnv(), "rpcEnv has not been initialized");
        return rpcEnv().endpointRef(this);
    }

    /**
     * Process messages from {@link RpcEndpointRef#send} or {@link RpcCallContext#reply}. If receiving an
     * unmatched message, {@link MoraxException} will be thrown and sent to {@link #onError}.
     *
     * @return whether the message is supported
     */
    default boolean receive(Object message) {
        throw new MoraxException(self() + " does not implement 'receive'");
    }

    /**
     * Process messages from {@link RpcEndpointRef#ask}. If receiving an unmatched message,
     * {@link MoraxException} will be thrown and sent to {@link #onError}.
     *
     * @return whether the message is supported
     */
    default boolean receiveAndReply(Object message, RpcCallContext context) {
        context.sendFailure(new MoraxException(self() + " won't reply anything"));
        return true;
    }

    /**
     * Invoked when any exception is thrown during handling messages.
     */
    default void onError(Throwable cause) throws Throwable {
        // By default, throw e and let RpcEnv handle it
        throw cause;
    }

    /**
     * Invoked when remoteAddress is connected to the current node.
     */
    default void onConnected(RpcAddress remoteAddress) {
        // By default, do nothing.
    }

    /**
     * Invoked when remoteAddress is lost.
     */
    default void onDisconnected(RpcAddress remoteAddress) {
        // By default, do nothing.
    }

    /**
     * Invoked when some network error happens in the connection between the current node and remoteAddress.
     */
    default void onNetworkError(Throwable cause, RpcAddress remoteAddress) {
        // By default, do nothing.
    }

    /**
     * Invoked before {@link RpcEndpoint} starts to handle any message.
     */
    default void onStart() {
        // By default, do nothing.
    }

    /**
     * Invoked when {@link RpcEndpoint} is stopping. {@link #self} will be {@code null} in this method, and you cannot
     * use it to send or ask messages.
     */
    default void onStop() {
        // By default, do nothing.
    }

    /**
     * A convenient method to stop {@link RpcEndpoint}.
     */
    default void stop() {
        final RpcEndpointRef self = self();
        if (self != null) {
            rpcEnv().stop(self);
        }
    }
}
