package org.tisonkun.nymph.rpc.netty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import org.tisonkun.nymph.rpc.RpcAddress;
import org.tisonkun.nymph.rpc.RpcEndpoint;
import org.tisonkun.nymph.rpc.RpcEndpointRef;
import org.tisonkun.nymph.rpc.RpcEnv;
import org.tisonkun.nymph.rpc.network.client.TransportClient;
import org.tisonkun.nymph.util.ThreadUtils;

public class NettyRpcEnv extends RpcEnv {
    // Because TransportClientFactory.createClient is blocking, we need to run it in this thread pool
    // to implement non-blocking send/ask.
    @Getter(AccessLevel.MODULE)
    private final ExecutorService clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool("netty-rpc-connection", 64);

    /**
     * A map for [[RpcAddress]] and [[Outbox]]. When we are connecting to a remote [[RpcAddress]],
     * we just put messages to its [[Outbox]] to implement a non-blocking `send` method.
     */
    private final ConcurrentMap<RpcAddress, Outbox> outboxes = new ConcurrentHashMap<>();

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

    public TransportClient createClient(RpcAddress address) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("not yet");
    }

    /**
     * Remove the address's Outbox and stop it.
     */
    public void removeOutbox(RpcAddress address) {
        final Outbox outbox = outboxes.remove(address);
        if (outbox != null) {
            outbox.stop();
        }
    }

    // TODO(@tison) replace with more generic and effective serialization.
    @SneakyThrows
    public ByteBuffer serialize(Object content) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(content);
        oos.close();
        return ByteBuffer.wrap(bos.toByteArray());
    }
}
