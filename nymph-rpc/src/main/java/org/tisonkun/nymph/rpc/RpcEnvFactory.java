package org.tisonkun.nymph.rpc;

import org.tisonkun.nymph.rpc.netty.NettyRpcEnvFactory;

/**
 * A factory class to create the {@link RpcEnv}. It must have an empty constructor so that it can be
 * created using Reflection.
 */
public interface RpcEnvFactory {
    RpcEnv create(RpcEnvConfig config);

    static RpcEnv create(String name, String bindAddress, String advertiseAddress, int port) {
        final RpcEnvConfig config = new RpcEnvConfig(name, bindAddress, advertiseAddress, port);
        return new NettyRpcEnvFactory().create(config);
    }
}
