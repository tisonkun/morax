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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.rpc.RpcAddress;
import org.tisonkun.morax.rpc.network.client.RpcResponseCallback;
import org.tisonkun.morax.rpc.network.client.TransportClient;
import org.tisonkun.morax.rpc.network.server.OneForOneStreamManager;
import org.tisonkun.morax.rpc.network.server.RpcHandler;
import org.tisonkun.morax.rpc.network.server.StreamManager;

/**
 * Dispatches incoming RPCs to registered endpoints.
 * <p>
 * The handler keeps track of all client instances that communicate with it, so that the RpcEnv
 * knows which `TransportClient` instance to use when sending RPCs to a client endpoint (i.e.,
 * one that is not listening for incoming connections, but rather needs to be contacted via the
 * client socket).
 * <p>
 * Events are sent on a per-connection basis, so if a client opens multiple connections to the
 * RpcEnv, multiple connection / disconnection events will be created for that client (albeit
 * with different `RpcAddress` information).
 */
@Slf4j
public class NettyRpcHandler extends RpcHandler {
    private final StreamManager streamManager = new OneForOneStreamManager();

    // A variable to track the remote RpcEnv addresses of all clients
    private final ConcurrentMap<RpcAddress, RpcAddress> remoteAddresses = new ConcurrentHashMap<>();

    private final Dispatcher dispatcher;

    private final NettyRpcEnv nettyRpcEnv;

    public NettyRpcHandler(Dispatcher dispatcher, NettyRpcEnv nettyRpcEnv) {
        this.dispatcher = dispatcher;
        this.nettyRpcEnv = nettyRpcEnv;
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        final RequestMessage messageToDispatch = internalReceive(client, message);
        dispatcher.postRemoteMessage(messageToDispatch, callback);
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message) {
        final RequestMessage messageToDispatch = internalReceive(client, message);
        dispatcher.postOneWayMessage(messageToDispatch);
    }

    private RequestMessage internalReceive(TransportClient client, ByteBuffer message) {
        final InetSocketAddress addr = (InetSocketAddress) client.getChannel().remoteAddress();
        Preconditions.checkNotNull(addr);
        final RpcAddress clientAddr = new RpcAddress(addr.getHostString(), addr.getPort());
        final RequestMessage requestMessage = RequestMessage.create(nettyRpcEnv, client, message);
        // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
        // the listening address
        final RpcAddress remoteEnvAddress = requestMessage.senderAddress();
        if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
            dispatcher.postToAll(new InboxMessage.RemoteProcessConnected(remoteEnvAddress));
        }
        return requestMessage;
    }

    @Override
    public void channelActive(TransportClient client) {
        final InetSocketAddress addr = (InetSocketAddress) client.getChannel().remoteAddress();
        Preconditions.checkNotNull(addr);
        final RpcAddress clientAddr = new RpcAddress(addr.getHostString(), addr.getPort());
        dispatcher.postToAll(new InboxMessage.RemoteProcessConnected(clientAddr));
    }

    @Override
    public void channelInactive(TransportClient client) {
        final InetSocketAddress addr = (InetSocketAddress) client.getChannel().remoteAddress();
        if (addr != null) {
            final RpcAddress clientAddr = new RpcAddress(addr.getHostString(), addr.getPort());
            nettyRpcEnv.removeOutbox(clientAddr);
            dispatcher.postToAll(new InboxMessage.RemoteProcessDisconnected(clientAddr));
            final RpcAddress remoteEnvAddress = remoteAddresses.remove(clientAddr);
            // If the remove RpcEnv listens to some address, we should also  fire a
            // RemoteProcessDisconnected for the remote RpcEnv listening address
            if (remoteEnvAddress != null) {
                dispatcher.postToAll(new InboxMessage.RemoteProcessDisconnected(remoteEnvAddress));
            }
        }
        // else:
        //  If the channel is closed before connecting, its remoteAddress will be null. In this case,
        //  we can ignore it since we don't fire "Associated".
        //  See java.net.Socket.getRemoteSocketAddress
    }

    @Override
    public void exceptionCaught(Throwable cause, TransportClient client) {
        final InetSocketAddress addr = (InetSocketAddress) client.getChannel().remoteAddress();
        if (addr != null) {
            final RpcAddress clientAddr = new RpcAddress(addr.getHostString(), addr.getPort());
            dispatcher.postToAll(new InboxMessage.RemoteProcessConnectionError(cause, clientAddr));
            // If the remove RpcEnv listens to some address, we should also fire a
            // RemoteProcessConnectionError for the remote RpcEnv listening address
            final RpcAddress remoteEnvAddress = remoteAddresses.get(clientAddr);
            if (remoteEnvAddress != null) {
                dispatcher.postToAll(new InboxMessage.RemoteProcessConnectionError(cause, remoteEnvAddress));
            }
        } else {
            // If the channel is closed before connecting, its remoteAddress will be null.
            // See java.net.Socket.getRemoteSocketAddress
            // Because we cannot get a RpcAddress, just log it
            log.error("Exception before connecting to the client", cause);
        }
    }

    @Override
    public StreamManager getStreamManager() {
        return streamManager;
    }
}
