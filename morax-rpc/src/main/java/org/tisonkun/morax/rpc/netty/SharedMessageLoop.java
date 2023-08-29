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

package org.tisonkun.morax.rpc.netty;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.tisonkun.morax.rpc.RpcEndpoint;
import org.tisonkun.morax.util.ThreadUtils;

/**
 * A message loop that serves multiple RPC endpoints, using a shared thread pool.
 */
public final class SharedMessageLoop extends MessageLoop {

    // TODO(@tison) respect MoraxConfig
    private static int getNumOfThreads() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Thread pool used for dispatching messages.
     */
    private static ExecutorService executorService(int numThreads) {
        return ThreadUtils.newDaemonFixedThreadPool(numThreads, "dispatcher-event-loop");
    }

    private final ConcurrentMap<String, Inbox> endpoints = new ConcurrentHashMap<>();

    public SharedMessageLoop(Dispatcher dispatcher) {
        super(dispatcher, executorService(getNumOfThreads()));

        final int numThreads = getNumOfThreads();
        for (int i = 0; i < numThreads; i++) {
            executorService.execute(receiveLoopRunnable);
        }
    }

    @Override
    public void post(String endpointName, InboxMessage message) {
        final Inbox inbox = endpoints.get(endpointName);
        inbox.post(message);
        setActive(inbox);
    }

    @Override
    public void unregister(String endpointName) {
        final Inbox inbox = endpoints.remove(endpointName);
        if (inbox != null) {
            inbox.stop();
            // Mark active to handle the OnStop message.
            setActive(inbox);
        }
    }

    public void register(String name, RpcEndpoint endpoint) {
        final Inbox inbox = new Inbox(name, endpoint);
        endpoints.put(name, inbox);
        // Mark active to handle the OnStart message.
        setActive(inbox);
    }
}
