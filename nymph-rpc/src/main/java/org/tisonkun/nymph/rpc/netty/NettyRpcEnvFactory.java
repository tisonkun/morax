package org.tisonkun.nymph.rpc.netty;

import org.tisonkun.nymph.rpc.RpcEnv;
import org.tisonkun.nymph.rpc.RpcEnvConfig;
import org.tisonkun.nymph.rpc.RpcEnvFactory;

public class NettyRpcEnvFactory implements RpcEnvFactory {
    @Override
    public RpcEnv create(RpcEnvConfig config) {
        return new NettyRpcEnv(config);
    }
}
