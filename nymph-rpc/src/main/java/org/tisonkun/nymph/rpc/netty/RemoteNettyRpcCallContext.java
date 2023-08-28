package org.tisonkun.nymph.rpc.netty;

import java.nio.ByteBuffer;
import org.tisonkun.nymph.rpc.RpcAddress;
import org.tisonkun.nymph.rpc.network.client.RpcResponseCallback;

/**
 * A [[RpcCallContext]] that will call [[RpcResponseCallback]] to send the reply back.
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
