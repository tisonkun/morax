package org.tisonkun.nymph.rpc.netty;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.nymph.rpc.exception.RpcEnvStoppedException;
import org.tisonkun.nymph.rpc.network.client.RpcResponseCallback;
import org.tisonkun.nymph.rpc.network.client.TransportClient;

public sealed interface OutboxMessage {
    void sendWith(TransportClient client);

    void onFailure(Throwable t);

    @Slf4j
    record OneWay(ByteBuffer content) implements OutboxMessage {
        @Override
        public void sendWith(TransportClient client) {
            client.send(content);
        }

        @Override
        public void onFailure(Throwable t) {
            if (t instanceof RpcEnvStoppedException e) {
                log.debug(e.getMessage());
            } else {
                log.warn("Failed to send one-way RPC.", t);
            }
        }
    }

    @Slf4j
    final class Rpc implements OutboxMessage, RpcResponseCallback {
        private final ByteBuffer content;
        private final BiConsumer<TransportClient, ByteBuffer> onSuccess;
        private final Consumer<Throwable> onFailure;

        private TransportClient client;
        private long requestId;

        public Rpc(
                ByteBuffer content, BiConsumer<TransportClient, ByteBuffer> onSuccess, Consumer<Throwable> onFailure) {
            this.content = content;
            this.onSuccess = onSuccess;
            this.onFailure = onFailure;
        }

        @Override
        public void sendWith(TransportClient client) {
            this.client = client;
            this.requestId = client.sendRpc(content, this);
        }

        @Override
        public void onFailure(Throwable t) {
            onFailure.accept(t);
        }

        @Override
        public void onSuccess(ByteBuffer response) {
            onSuccess.accept(client, response);
        }

        public void onTimeout() {
            removeRpcRequest();
        }

        public void onAbort() {
            removeRpcRequest();
        }

        public void removeRpcRequest() {
            if (client != null) {
                client.removeRpcRequest(requestId);
            } else {
                log.error("Ask terminated before connecting successfully");
            }
        }
    }
}
