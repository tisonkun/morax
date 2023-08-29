package org.tisonkun.nymph.rpc.netty;

import org.tisonkun.nymph.rpc.RpcAddress;

public sealed interface InboxMessage {
    record OnStart() implements InboxMessage {}

    record OnStop() implements InboxMessage {}

    /**
     * A message to tell all endpoints that a remote process has connected.
     */
    record RemoteProcessConnected(RpcAddress remoteAddress) implements InboxMessage {}

    /**
     * A message to tell all endpoints that a remote process has disconnected.
     */
    record RemoteProcessDisconnected(RpcAddress remoteAddress) implements InboxMessage {}

    /**
     * A message to tell all endpoints that a network error has happened.
     */
    record RemoteProcessConnectionError(Throwable cause, RpcAddress remoteAddress) implements InboxMessage {}

    record OneWay(RpcAddress remoteAddress, Object content) implements InboxMessage {}

    record Rpc(RpcAddress remoteAddress, Object content, NettyRpcCallContext context) implements InboxMessage {}
}
