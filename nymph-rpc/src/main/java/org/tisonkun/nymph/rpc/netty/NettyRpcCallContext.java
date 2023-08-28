package org.tisonkun.nymph.rpc.netty;

import org.tisonkun.nymph.rpc.RpcAddress;
import org.tisonkun.nymph.rpc.RpcCallContext;

public abstract class NettyRpcCallContext implements RpcCallContext {
    private final RpcAddress senderAddress;

    public NettyRpcCallContext(RpcAddress senderAddress) {
        this.senderAddress = senderAddress;
    }

    protected abstract void send(Object message);

    @Override
    public void reply(Object response) {
        send(response);
    }

    @Override
    public void sendFailure(Throwable t) {
        send(new RpcFailure(t));
    }

    @Override
    public RpcAddress senderAddress() {
        return this.senderAddress;
    }
}
