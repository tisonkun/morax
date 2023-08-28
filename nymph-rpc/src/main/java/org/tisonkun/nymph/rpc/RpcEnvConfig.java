package org.tisonkun.nymph.rpc;

public record RpcEnvConfig(String name, String bindAddress, String advertiseAddress, int port) {
}
