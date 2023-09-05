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

import com.google.common.base.Preconditions;
import java.util.concurrent.ExecutorService;
import org.tisonkun.morax.rpc.IsolatedRpcEndpoint;
import org.tisonkun.morax.util.ThreadUtils;

public final class DedicatedMessageLoop extends MessageLoop {
    private static ExecutorService executorService(String name, IsolatedRpcEndpoint endpoint) {
        if (endpoint.threadCount() > 1) {
            return ThreadUtils.newDaemonCachedThreadPool("dispatcher-" + name, endpoint.threadCount());
        } else {
            return ThreadUtils.newDaemonSingleThreadExecutor("dispatcher-" + name);
        }
    }

    private final String name;
    private final Inbox inbox;

    public DedicatedMessageLoop(String name, IsolatedRpcEndpoint endpoint, Dispatcher dispatcher) {
        super(dispatcher, executorService(name, endpoint));
        this.name = name;
        this.inbox = new Inbox(name, endpoint);

        for (int i = 0; i < endpoint.threadCount(); i++) {
            // We need to be careful not to use ExecutorService#submit, because
            // `submit` api will swallow uncaught exceptions in FutureTask#setException.
            executorService.execute(receiveLoopRunnable);
        }

        // Mark active to handle the OnStart message.
        setActive(inbox);
    }

    @Override
    public void post(String endpointName, InboxMessage message) {
        Preconditions.checkState(name.equals(endpointName), "name=%s but endpointName=%s", name, endpointName);
        inbox.post(message);
        setActive(inbox);
    }

    @Override
    public void unregister(String endpointName) {
        synchronized (lock) {
            Preconditions.checkState(name.equals(endpointName), "name=%s but endpointName=%s", name, endpointName);
            inbox.stop();
            // Mark active to handle the OnStop message.
            setActive(inbox);
            setActive(POISON_PILL);
            executorService.shutdown();
        }
    }
}
