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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.tisonkun.morax.rpc.AbortableRpcFuture;
import org.tisonkun.morax.rpc.RpcAddress;
import org.tisonkun.morax.rpc.RpcEndpointAddress;
import org.tisonkun.morax.rpc.RpcEndpointRef;
import org.tisonkun.morax.rpc.RpcTimeout;
import org.tisonkun.morax.rpc.network.client.TransportClient;
import org.tisonkun.morax.util.ThrowableUtils;

/**
 * The NettyRpcEnv version of RpcEndpointRef.
 * <p>
 * This class behaves differently depending on where it's created. On the node that "owns" the
 * RpcEndpoint, it's a simple wrapper around the RpcEndpointAddress instance.
 * <p>
 * On other machines that receive a serialized version of the reference, the behavior changes. The
 * instance will keep track of the TransportClient that sent the reference, so that messages
 * to the endpoint are sent over the client connection, instead of needing a new connection to
 * be opened.
 */
public class NettyRpcEndpointRef extends RpcEndpointRef implements Serializable {
    private final RpcEndpointAddress endpointAddress;

    private transient volatile NettyRpcEnv nettyRpcEnv;

    @Getter(AccessLevel.MODULE)
    @Setter(AccessLevel.MODULE)
    private transient volatile TransportClient client;

    public NettyRpcEndpointRef(RpcEndpointAddress endpointAddress, NettyRpcEnv nettyRpcEnv) {
        this.endpointAddress = endpointAddress;
        this.nettyRpcEnv = nettyRpcEnv;
    }

    @Override
    public RpcAddress address() {
        return endpointAddress.rpcAddress();
    }

    @Serial
    private void readObject(ObjectInputStream in) throws IOException {
        try {
            in.defaultReadObject();
            this.nettyRpcEnv = NettyRpcEnv.CURRENT_ENV.getValue();
            this.client = NettyRpcEnv.CURRENT_CLIENT.getValue();
        } catch (ClassNotFoundException e) {
            throw ThrowableUtils.sneakyThrow(e);
        }
    }

    @Serial
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }

    @Override
    public String name() {
        return endpointAddress.name();
    }

    @Override
    public void send(Object message) {
        Preconditions.checkNotNull(message, "Message is null");
        nettyRpcEnv.send(new RequestMessage(nettyRpcEnv.address(), this, message));
    }

    @Override
    public <T> AbortableRpcFuture<T> askAbortable(Object message, RpcTimeout timeout) {
        return nettyRpcEnv.askAbortable(new RequestMessage(nettyRpcEnv.address(), this, message), timeout);
    }

    @Override
    public <T> CompletableFuture<T> ask(Object message, RpcTimeout timeout) {
        final AbortableRpcFuture<T> future = askAbortable(message, timeout);
        return future.getFuture();
    }

    @Override
    public String toString() {
        return "NettyRpcEndpointRef(%s)".formatted(endpointAddress);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NettyRpcEndpointRef other) {
            return other.endpointAddress.equals(this.endpointAddress);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return endpointAddress != null ? endpointAddress.hashCode() : 0;
    }
}
