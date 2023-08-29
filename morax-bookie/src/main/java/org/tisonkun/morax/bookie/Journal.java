/*
 * Copyright 2023 tison <wander4096@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tisonkun.morax.bookie;

import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.rpc.RpcCallContext;
import org.tisonkun.morax.rpc.RpcEnv;
import org.tisonkun.morax.rpc.RpcEnvFactory;
import org.tisonkun.morax.rpc.ThreadSafeRpcEndpoint;

@Slf4j
public class Journal {
    private static class TestRpcEndpoint implements ThreadSafeRpcEndpoint {
        @Override
        public RpcEnv rpcEnv() {
            return null;
        }

        @Override
        public boolean receive(Object message) {
            log.error("receive message = " + message);
            return true;
        }

        @Override
        public boolean receiveAndReply(Object message, RpcCallContext context) {
            log.error("receiveAndReply message = " + message);
            context.reply("Reply!");
            return true;
        }
    }

    public static void main(String[] args) {
        final var env = RpcEnvFactory.create("main", "localhost", "localhost", 0);
        final var env2 = RpcEnvFactory.create("main2", "localhost", "localhost", 0);
        final var ref = env.setupEndpoint("ref", new TestRpcEndpoint());
        final var ref2 = env2.setupEndpointRef(ref.address(), ref.name());
        ref2.send("Send!");
        ref2.ask("Ask!").join();
    }
}
