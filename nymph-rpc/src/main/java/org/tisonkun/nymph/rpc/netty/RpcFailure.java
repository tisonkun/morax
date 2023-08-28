package org.tisonkun.nymph.rpc.netty;

/**
 * A response that indicates some failure happens in the receiver side.
 */
public record RpcFailure(Throwable t) {}
