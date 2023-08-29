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

import java.io.Serial;
import java.io.Serializable;
import org.tisonkun.morax.rpc.RpcCallContext;
import org.tisonkun.morax.rpc.RpcEndpoint;
import org.tisonkun.morax.rpc.RpcEnv;

/**
 * An {@link RpcEndpoint} for remote {@link RpcEnv}s to query if an `RpcEndpoint` exists.
 * <p>
 * This is used when setting up a remote endpoint reference.
 */
public class RpcEndpointVerifier implements RpcEndpoint {
    public static final String NAME = "endpoint-verifier";

    /**
     * A message used to ask the remote {@link RpcEndpointVerifier} if an RpcEndpoint exists.
     */
    public record CheckExistence(String name) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }

    private final Dispatcher dispatcher;
    private final RpcEnv rpcEnv;

    public RpcEndpointVerifier(RpcEnv rpcEnv, Dispatcher dispatcher) {
        this.rpcEnv = rpcEnv;
        this.dispatcher = dispatcher;
    }

    @Override
    public RpcEnv rpcEnv() {
        return rpcEnv;
    }

    @Override
    public boolean receiveAndReply(Object message, RpcCallContext context) {
        if (message instanceof RpcEndpointVerifier.CheckExistence m) {
            context.reply(dispatcher.verify(m.name));
            return true;
        }
        return false;
    }
}
