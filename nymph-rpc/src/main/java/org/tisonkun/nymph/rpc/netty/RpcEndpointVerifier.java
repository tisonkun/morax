package org.tisonkun.nymph.rpc.netty;

import java.io.Serial;
import java.io.Serializable;
import org.tisonkun.nymph.rpc.RpcCallContext;
import org.tisonkun.nymph.rpc.RpcEndpoint;
import org.tisonkun.nymph.rpc.RpcEnv;

/**
 * An [[RpcEndpoint]] for remote [[RpcEnv]]s to query if an `RpcEndpoint` exists.
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
