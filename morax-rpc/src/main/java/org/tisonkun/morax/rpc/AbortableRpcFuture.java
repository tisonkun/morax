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

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import lombok.Getter;

/**
 * A subclass of {@link CompletableFuture} that adds abort method.
 * This is used in long run RPC and provide an approach to abort the RPC.
 */
public class AbortableRpcFuture<T> {
    @Getter
    private final CompletableFuture<T> future;

    private final Consumer<Throwable> onAbort;

    public AbortableRpcFuture(CompletableFuture<T> future, Consumer<Throwable> onAbort) {
        this.future = future;
        this.onAbort = onAbort;
    }

    public void abort(Throwable t) {
        onAbort.accept(t);
    }
}
