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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.tisonkun.morax.io.ByteBufferInputStream;
import org.tisonkun.morax.io.ByteBufferOutputStream;
import org.tisonkun.morax.rpc.RpcAddress;
import org.tisonkun.morax.rpc.RpcEndpointAddress;
import org.tisonkun.morax.rpc.network.client.TransportClient;
import org.tisonkun.morax.util.ThrowableUtils;

/**
 * The message that is sent from the sender to the receiver.
 *
 * @param senderAddress the sender address. It's {@code null} if this message is from a client
 *                      `NettyRpcEnv`.
 * @param receiver      the receiver of this message.
 * @param content       the message content.
 */
// TODO(@tison) replace with more generic and effective serialization.
public record RequestMessage(RpcAddress senderAddress, NettyRpcEndpointRef receiver, Object content) {
    private static RpcAddress readRpcAddress(DataInputStream in) throws IOException {
        final boolean hasRpcAddress = in.readBoolean();
        if (hasRpcAddress) {
            final String host = in.readUTF();
            final int port = in.readInt();
            return new RpcAddress(host, port);
        } else {
            return null;
        }
    }

    public static RequestMessage create(NettyRpcEnv nettyRpcEnv, TransportClient client, ByteBuffer bytes) {
        final ByteBufferInputStream bis = new ByteBufferInputStream(bytes);
        try (final DataInputStream in = new DataInputStream(bis)) {
            final RpcAddress senderAddress = readRpcAddress(in);
            final RpcEndpointAddress endpointAddress = new RpcEndpointAddress(readRpcAddress(in), in.readUTF());
            final NettyRpcEndpointRef ref = new NettyRpcEndpointRef(endpointAddress, nettyRpcEnv);
            ref.setClient(client);
            try (final ObjectInputStream ois = new ObjectInputStream(in)) {
                return new RequestMessage(senderAddress, ref, ois.readObject());
            } catch (ClassNotFoundException e) {
                throw ThrowableUtils.sneakyThrow(e);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Manually serialize {@link RequestMessage} to minimize the size.
     */
    public ByteBuffer serialize() {
        final ByteBufferOutputStream bos = new ByteBufferOutputStream();
        try (final DataOutputStream out = new DataOutputStream(bos)) {
            writeRpcAddress(out, senderAddress);
            writeRpcAddress(out, receiver.address());
            out.writeUTF(receiver.name());
            try (final ObjectOutputStream oos = new ObjectOutputStream(out)) {
                oos.writeObject(content);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return bos.toByteBuffer();
    }

    private void writeRpcAddress(DataOutputStream out, RpcAddress rpcAddress) throws IOException {
        if (rpcAddress == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(rpcAddress.host());
            out.writeInt(rpcAddress.port());
        }
    }
}
