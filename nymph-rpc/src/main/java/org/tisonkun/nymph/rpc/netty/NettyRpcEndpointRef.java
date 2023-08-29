package org.tisonkun.nymph.rpc.netty;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.tisonkun.nymph.rpc.AbortableRpcFuture;
import org.tisonkun.nymph.rpc.RpcAddress;
import org.tisonkun.nymph.rpc.RpcEndpointAddress;
import org.tisonkun.nymph.rpc.RpcEndpointRef;
import org.tisonkun.nymph.rpc.RpcTimeout;
import org.tisonkun.nymph.rpc.network.client.TransportClient;
import org.tisonkun.nymph.util.ThrowableUtils;

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
public class NettyRpcEndpointRef extends RpcEndpointRef {
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

    private void readObject(ObjectInputStream in) throws IOException {
        try {
            in.defaultReadObject();
            this.nettyRpcEnv = NettyRpcEnv.CURRENT_ENV.getValue();
            this.client = NettyRpcEnv.CURRENT_CLIENT.getValue();
        } catch (ClassNotFoundException e) {
            throw ThrowableUtils.sneakyThrow(e);
        }
    }

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
