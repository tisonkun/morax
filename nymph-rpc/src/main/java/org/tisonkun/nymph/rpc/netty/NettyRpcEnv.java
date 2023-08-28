package org.tisonkun.nymph.rpc.netty;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.tisonkun.nymph.rpc.RpcAddress;
import org.tisonkun.nymph.rpc.RpcEndpoint;
import org.tisonkun.nymph.rpc.RpcEndpointRef;
import org.tisonkun.nymph.rpc.RpcEnv;

public class NettyRpcEnv extends RpcEnv {
    @Override
    public RpcEndpointRef endpointRef(RpcEndpoint endpoint) {
        return null;
    }

    @Override
    public RpcAddress address() {
        return null;
    }

    @Override
    public RpcEndpointRef setupEndpoint(String name, RpcEndpoint endpoint) {
        return null;
    }

    @Override
    public CompletableFuture<RpcEndpointRef> asyncSetupEndpointRefByURI(String uri) {
        return null;
    }

    @Override
    public void stop(RpcEndpointRef endpointRef) {}

    @Override
    public void shutdown() {}

    @Override
    public void awaitTermination() {}

    @SneakyThrows
    public ByteBuffer serialize(Object content) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(content);
        oos.close();
        return ByteBuffer.wrap(bos.toByteArray());
    }
}
