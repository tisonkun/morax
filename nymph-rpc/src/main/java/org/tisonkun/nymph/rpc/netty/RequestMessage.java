package org.tisonkun.nymph.rpc.netty;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.tisonkun.nymph.rpc.RpcAddress;
import org.tisonkun.nymph.rpc.RpcEndpointAddress;
import org.tisonkun.nymph.rpc.network.client.TransportClient;
import org.tisonkun.nymph.util.ThrowableUtils;

/**
 * The message that is sent from the sender to the receiver.
 *
 * @param senderAddress the sender address. It's `null` if this message is from a client
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
        final ByteArrayInputStream bis = new ByteArrayInputStream(bytes.array());
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
     * Manually serialize [[RequestMessage]] to minimize the size.
     */
    public ByteBuffer serialize() {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
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

        return ByteBuffer.wrap(bos.toByteArray());
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
