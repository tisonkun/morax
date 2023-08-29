/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tisonkun.morax.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.commons.lang3.tuple.Pair;

public class TestRpcEndpoint implements ThreadSafeRpcEndpoint {
    private final Deque<Object> receiveMessages = new ArrayDeque<>();
    private final Deque<Object> receiveAndReplyMessages = new ArrayDeque<>();
    private final Deque<RpcAddress> onConnectedMessages = new ArrayDeque<>();
    private final Deque<RpcAddress> onDisconnectedMessages = new ArrayDeque<>();
    private final Deque<Pair<Throwable, RpcAddress>> onNetworkErrorMessages = new ArrayDeque<>();

    private volatile boolean started = false;
    private volatile boolean stopped = false;

    private final RpcEnv env;

    public TestRpcEndpoint() {
        this.env = null;
    }

    public TestRpcEndpoint(RpcEnv env) {
        this.env = env;
    }

    @Override
    public RpcEnv rpcEnv() {
        return env;
    }

    @Override
    public boolean receive(Object message) {
        receiveMessages.addLast(message);
        return true;
    }

    @Override
    public boolean receiveAndReply(Object message, RpcCallContext context) {
        receiveAndReplyMessages.addLast(message);
        return true;
    }

    @Override
    public void onConnected(RpcAddress remoteAddress) {
        onConnectedMessages.addLast(remoteAddress);
    }

    @Override
    public void onDisconnected(RpcAddress remoteAddress) {
        onDisconnectedMessages.addLast(remoteAddress);
    }

    @Override
    public void onNetworkError(Throwable cause, RpcAddress remoteAddress) {
        onNetworkErrorMessages.addLast(Pair.of(cause, remoteAddress));
    }

    public int numReceiveMessages() {
        return receiveMessages.size();
    }

    @Override
    public void onStart() {
        started = true;
    }

    @Override
    public void onStop() {
        stopped = true;
    }

    public void verifyStarted() {
        assertThat(started).describedAs("RpcEndpoint is not started").isTrue();
    }

    public void verifyStopped() {
        assertThat(stopped).describedAs("RpcEndpoint is not stopped").isTrue();
    }

    public void verifyReceiveMessages(Object... expected) {
        assertThat(expected.length).isLessThanOrEqualTo(receiveMessages.size());
        for (Object message : expected) {
            final Object actual = receiveMessages.pollFirst();
            assertThat(actual).isEqualTo(message);
        }
    }

    public void verifyReceiveAndReplyMessages(Object... expected) {
        assertThat(expected.length).isLessThanOrEqualTo(receiveAndReplyMessages.size());
        for (Object message : expected) {
            final Object actual = receiveAndReplyMessages.pollFirst();
            assertThat(actual).isEqualTo(message);
        }
    }

    public void verifyOnConnectedMessages(RpcAddress... expected) {
        assertThat(expected.length).isLessThanOrEqualTo(onConnectedMessages.size());
        for (Object message : expected) {
            final RpcAddress actual = onConnectedMessages.pollFirst();
            assertThat(actual).isEqualTo(message);
        }
    }

    public void verifyOnDisconnectedMessages(RpcAddress... expected) {
        assertThat(expected.length).isLessThanOrEqualTo(onDisconnectedMessages.size());
        for (Object message : expected) {
            final RpcAddress actual = onDisconnectedMessages.pollFirst();
            assertThat(actual).isEqualTo(message);
        }
    }

    public void verifyOnNetworkErrorMessages(Pair<Throwable, RpcAddress>... expected) {
        assertThat(expected.length).isLessThanOrEqualTo(onNetworkErrorMessages.size());
        for (Object message : expected) {
            final Pair<Throwable, RpcAddress> actual = onNetworkErrorMessages.pollFirst();
            assertThat(actual).isEqualTo(message);
        }
    }
}
