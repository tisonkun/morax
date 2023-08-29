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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public abstract class RpcEndpointRef {
    private final RpcTimeout defaultAskTimeout = new RpcTimeout(
            // TODO(@tison) respect MoraxConfig
            Duration.ofSeconds(5), "morax.rpc.askTimeout");

    /**
     * Return the address for this.
     */
    public abstract RpcAddress address();

    public abstract String name();

    /**
     * Sends a one-way asynchronous message. Fire-and-forget semantics.
     */
    public abstract void send(Object message);

    public abstract <T> AbortableRpcFuture<T> askAbortable(Object message, RpcTimeout timeout);

    public abstract <T> CompletableFuture<T> ask(Object message, RpcTimeout timeout);

    public final <T> CompletableFuture<T> ask(Object message) {
        return ask(message, defaultAskTimeout);
    }

    public final <T> T askSync(Object message, RpcTimeout timeout) {
        return timeout.awaitResult(ask(message, timeout));
    }

    public final <T> T askSync(Object message) {
        return askSync(message, defaultAskTimeout);
    }
}
