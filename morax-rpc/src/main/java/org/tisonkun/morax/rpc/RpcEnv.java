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
import java.util.function.Supplier;

public abstract class RpcEnv {
    private final RpcTimeout defaultLookupTimeout = new RpcTimeout(
            // TODO(@tison) respect MoraxConfig
            Duration.ofSeconds(30), "morax.rpc.lookupTimeout");

    /**
     * Return RpcEndpointRef of the registered {@link RpcEndpoint}. Will be used to implement
     * {@link RpcEndpoint#self()}. Return {@code null} if the corresponding {@link RpcEndpointRef}
     * does not exist.
     */
    public abstract RpcEndpointRef endpointRef(RpcEndpoint endpoint);

    /**
     * Return the address that {@link RpcEnv} is listening to.
     */
    public abstract RpcAddress address();

    /**
     * Register a {@link RpcEndpoint} with a name and return its {@link RpcEndpointRef}. {@link RpcEnv} does not
     * guarantee thread-safety.
     */
    public abstract RpcEndpointRef setupEndpoint(String name, RpcEndpoint endpoint);

    /**
     * Retrieve the {@link RpcEndpointRef} represented by `uri` asynchronously.
     */
    public abstract CompletableFuture<RpcEndpointRef> asyncSetupEndpointRefByURI(String uri);

    /**
     * Retrieve the {@link RpcEndpointRef} represented by `uri`. This is a blocking action.
     */
    public final RpcEndpointRef setupEndpointRefByURI(String uri) {
        return defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri));
    }

    /**
     * Retrieve the {@link RpcEndpointRef} represented by `address` and `endpointName`.
     * This is a blocking action.
     */
    public final RpcEndpointRef setupEndpointRef(RpcAddress address, String endpointName) {
        return setupEndpointRefByURI(new RpcEndpointAddress(address, endpointName).toString());
    }

    /**
     * Stop {@link RpcEndpoint} specified by `endpoint`.
     */
    public abstract void stop(RpcEndpointRef endpointRef);

    /**
     * Shutdown this {@link RpcEnv} asynchronously. If you need to make sure {@link RpcEnv} exits successfully,
     * call {@code awaitTermination()} straight after {@code shutdown()}.
     */
    public abstract void shutdown();

    /**
     * Wait until {@link RpcEnv} exits.
     */
    public abstract void awaitTermination();

    /**
     * {@link RpcEndpointRef} cannot be deserialized without {@link RpcEnv}. So when deserializing any object
     * that contains {@link RpcEndpointRef}s, the deserialization codes should be wrapped by this method.
     */
    public abstract <T> T deserialize(Supplier<T> deserializationAction);
}
