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

/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 * <p>
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same {@link ThreadSafeRpcEndpoint}. In the other words, changes to internal fields of a
 * {@link ThreadSafeRpcEndpoint} are visible when processing the next message, and fields in the
 * {@link ThreadSafeRpcEndpoint} need not be volatile or equivalent.
 * <p>
 * However, there is no guarantee that the same thread will be executing the same
 * {@link ThreadSafeRpcEndpoint} for different messages.
 */
public interface ThreadSafeRpcEndpoint extends RpcEndpoint {}
