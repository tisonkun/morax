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

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.tisonkun.morax.rpc.exception.RpcTimeoutException;
import org.tisonkun.morax.util.ThrowableUtils;

/**
 * Associates a timeout with a description so that a when a TimeoutException occurs, additional
 * context about the timeout can be amended to the exception message.
 *
 * @param duration    timeout duration in seconds
 * @param timeoutProp the configuration property that controls this timeout
 */
public record RpcTimeout(Duration duration, String timeoutProp) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Amends the standard message of TimeoutException to include the description
     */
    private RpcTimeoutException createRpcTimeoutException(TimeoutException te) {
        return new RpcTimeoutException(te.getMessage() + ". This timeout is controlled by " + timeoutProp, te);
    }

    /**
     * Match a TimeoutException and add the timeout description to the message.
     * <p>
     * This can be used in the recover callback of a Future to add to a TimeoutException.
     */
    public <T> T addMessageIfTimeout(Throwable t) {
        final Throwable throwable = ThrowableUtils.stripCompletionException(t);
        if (throwable instanceof TimeoutException te) {
            throw ThrowableUtils.sneakyThrow(createRpcTimeoutException(te));
        } else {
            throw ThrowableUtils.sneakyThrow(throwable);
        }
    }

    /**
     * Wait for the completed result and return it. If the result is not available within this
     * timeout, throw a {@link RpcTimeoutException} to indicate which configuration controls the timeout.
     *
     * @param future the Future to be awaited
     */
    public <T> T awaitResult(CompletableFuture<T> future) {
        try {
            return future.orTimeout(duration.toMillis(), TimeUnit.MILLISECONDS)
                    .exceptionally(this::addMessageIfTimeout)
                    .join();
        } catch (CompletionException e) {
            throw ThrowableUtils.sneakyThrow(ThrowableUtils.stripCompletionException(e));
        }
    }
}
