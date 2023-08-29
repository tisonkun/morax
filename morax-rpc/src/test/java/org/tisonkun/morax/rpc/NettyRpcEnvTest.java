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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.tisonkun.morax.rpc.exception.RpcTimeoutException;
import org.tisonkun.morax.util.ThrowableUtils;

class NettyRpcEnvTest {
    private static final RpcEnv env = createEnv("test");

    @AfterAll
    public static void teardown() {
        env.shutdown();
    }

    private static RpcEnv createEnv(String name) {
        return RpcEnvFactory.create(name, "localhost", "localhost", 0);
    }

    @Test
    void testSendMessageLocally() throws Exception {
        final CompletableFuture<String> f = new CompletableFuture<>();
        final RpcEndpointRef ref = env.setupEndpoint("send-locally", new TestRpcEndpoint(env) {
            @Override
            public boolean receive(Object message) {
                if (message instanceof String m) {
                    f.complete(m);
                }
                return super.receive(message);
            }
        });
        ref.send("hello");
        assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo("hello");
    }

    @Test
    void testSendMessageRemotely() throws Exception {
        final CompletableFuture<String> f = new CompletableFuture<>();
        env.setupEndpoint("send-remotely", new TestRpcEndpoint(env) {
            @Override
            public boolean receive(Object message) {
                if (message instanceof String m) {
                    f.complete(m);
                }
                return super.receive(message);
            }
        });

        final RpcEnv anotherEnv = createEnv("remote");
        // Use anotherEnv to find out the RpcEndpointRef
        final RpcEndpointRef ref = anotherEnv.setupEndpointRef(env.address(), "send-remotely");
        try {
            ref.send("hello");
            assertThat(f.get(5, TimeUnit.SECONDS)).isEqualTo("hello");
        } finally {
            anotherEnv.shutdown();
            anotherEnv.awaitTermination();
        }
    }

    @Test
    void testSendRpcEndpointRef() {
        final RpcEndpoint endpoint = new TestRpcEndpoint(env) {
            @Override
            public boolean receiveAndReply(Object message, RpcCallContext context) {
                if (message instanceof String m) {
                    switch (m) {
                        case "Hello" -> context.reply(self());
                        case "Echo" -> context.reply("Echo");
                    }
                }
                return super.receiveAndReply(message, context);
            }
        };
        final RpcEndpointRef rpcEndpointRef = env.setupEndpoint("send-ref", endpoint);
        final RpcEndpointRef newRpcEndpointRef = rpcEndpointRef.askSync("Hello");
        final String reply = newRpcEndpointRef.askSync("Echo");
        assertThat(reply).isEqualTo("Echo");
    }

    @Test
    void testAskMessageLocally() {
        final RpcEndpointRef ref = env.setupEndpoint("ask-locally", new TestRpcEndpoint(env) {
            @Override
            public boolean receiveAndReply(Object message, RpcCallContext context) {
                if (message instanceof String m) {
                    context.reply(m);
                }
                return super.receiveAndReply(message, context);
            }
        });
        final String reply = ref.askSync("Echo");
        assertThat(reply).isEqualTo("Echo");
    }

    @Test
    void testAskMessageRemotely() {
        env.setupEndpoint("ask-remotely", new TestRpcEndpoint(env) {
            @Override
            public boolean receiveAndReply(Object message, RpcCallContext context) {
                if (message instanceof String m) {
                    context.reply(m);
                }
                return super.receiveAndReply(message, context);
            }
        });
        final RpcEnv anotherEnv = createEnv("remote");
        // Use anotherEnv to find out the RpcEndpointRef
        final RpcEndpointRef ref = anotherEnv.setupEndpointRef(env.address(), "ask-remotely");
        try {
            final String reply = ref.askSync("hello");
            assertThat(reply).isEqualTo("hello");
        } finally {
            anotherEnv.shutdown();
            anotherEnv.awaitTermination();
        }
    }

    @Test
    void testAskMessageTimeout() {
        env.setupEndpoint("ask-timeout", new TestRpcEndpoint(env) {
            @Override
            public boolean receiveAndReply(Object message, RpcCallContext context) {
                if (message instanceof String m) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw ThrowableUtils.sneakyThrow(e);
                    }
                    context.reply(m);
                }
                return super.receiveAndReply(message, context);
            }
        });
        final RpcEnv anotherEnv = createEnv("remote");
        // Use anotherEnv to find out the RpcEndpointRef
        final RpcEndpointRef ref = anotherEnv.setupEndpointRef(env.address(), "ask-timeout");
        final String shortProp = "morax.rpc.short.timeout";
        try {
            assertThatThrownBy(() -> ref.askSync("hello", new RpcTimeout(Duration.ofMillis(1), shortProp)))
                    .isInstanceOf(RpcTimeoutException.class)
                    .hasMessageContaining(shortProp);
        } finally {
            anotherEnv.shutdown();
            anotherEnv.awaitTermination();
        }
    }

    @Test
    void testAskMessageAbort() {
        env.setupEndpoint("ask-abort", new TestRpcEndpoint(env) {
            @Override
            public boolean receiveAndReply(Object message, RpcCallContext context) {
                if (message instanceof String m) {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        throw ThrowableUtils.sneakyThrow(e);
                    }
                    context.reply(m);
                }
                return super.receiveAndReply(message, context);
            }
        });
        final RpcEnv anotherEnv = createEnv("remote");
        // Use anotherEnv to find out the RpcEndpointRef
        final RpcEndpointRef ref = anotherEnv.setupEndpointRef(env.address(), "ask-abort");
        final String shortProp = "morax.rpc.short.timeout";
        try {
            assertThatThrownBy(() -> {
                        final RpcTimeout timeout = new RpcTimeout(Duration.ofSeconds(10), shortProp);
                        final AbortableRpcFuture<?> f = ref.askAbortable("hello", timeout);
                        new Thread(() -> {
                                    try {
                                        Thread.sleep(100);
                                    } catch (InterruptedException e) {
                                        throw ThrowableUtils.sneakyThrow(e);
                                    }
                                    f.abort(new RuntimeException("TestAbort"));
                                })
                                .start();
                        timeout.awaitResult(f.getFuture());
                    })
                    .isExactlyInstanceOf(RuntimeException.class)
                    .hasMessage("TestAbort");
        } finally {
            anotherEnv.shutdown();
            anotherEnv.awaitTermination();
        }
    }
}
