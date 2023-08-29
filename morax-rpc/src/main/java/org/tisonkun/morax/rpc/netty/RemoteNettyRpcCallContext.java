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

import java.nio.ByteBuffer;
import org.tisonkun.morax.rpc.RpcAddress;
import org.tisonkun.morax.rpc.RpcCallContext;
import org.tisonkun.morax.rpc.network.client.RpcResponseCallback;

/**
 * A {@link RpcCallContext} that will call {@link RpcResponseCallback} to send the reply back.
 */
public class RemoteNettyRpcCallContext extends NettyRpcCallContext {
    private final NettyRpcEnv nettyRpcEnv;
    private final RpcResponseCallback callback;

    public RemoteNettyRpcCallContext(RpcAddress senderAddress, NettyRpcEnv nettyRpcEnv, RpcResponseCallback callback) {
        super(senderAddress);
        this.nettyRpcEnv = nettyRpcEnv;
        this.callback = callback;
    }

    @Override
    protected void send(Object message) {
        final ByteBuffer reply = nettyRpcEnv.serialize(message);
        callback.onSuccess(reply);
    }
}
