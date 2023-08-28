package org.tisonkun.nymph.rpc;

/**
 * A factory class to create the {@link RpcEnv}. It must have an empty constructor so that it can be
 * created using Reflection.
 */
public interface RpcEnvFactory {
    RpcEnv create(RpcEnvConfig config);
}
