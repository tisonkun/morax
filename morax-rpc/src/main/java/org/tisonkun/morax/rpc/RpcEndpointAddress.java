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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.net.URI;
import java.net.URISyntaxException;
import javax.annotation.Nullable;
import org.tisonkun.morax.exception.MoraxException;
import org.tisonkun.morax.rpc.netty.NettyRpcEnv;

/**
 * An address identifier for an RPC endpoint.
 *
 * @param rpcAddress The socket address of the endpoint. It's {@code null} when this address pointing to
 *                   an endpoint in a client {@link NettyRpcEnv}.
 * @param name       Name of the endpoint.
 */
public record RpcEndpointAddress(@Nullable RpcAddress rpcAddress, String name) {
    public static RpcEndpointAddress create(String url) {
        try {
            final var uri = new URI(url);
            final var host = uri.getHost();
            final var port = uri.getPort();
            final var name = uri.getUserInfo();

            // ensure in form "morax://$name@${host}:${port}"
            if (uri.getScheme().equals("morax")
                    && host != null
                    && name != null
                    && port >= 0
                    && Strings.isNullOrEmpty(uri.getPath())
                    && Strings.isNullOrEmpty(uri.getFragment())
                    && Strings.isNullOrEmpty(uri.getQuery())) {
                return new RpcEndpointAddress(host, port, name);
            } else {
                throw new MoraxException("Invalid Morax URL: " + url);
            }
        } catch (URISyntaxException e) {
            throw new MoraxException("Invalid Morax URL: " + url, e);
        }
    }

    public RpcEndpointAddress {
        Preconditions.checkNotNull(name, "RpcEndpoint name must be provided.");
    }

    public RpcEndpointAddress(String host, int port, String name) {
        this(new RpcAddress(host, port), name);
    }

    @Override
    public String toString() {
        if (rpcAddress != null) {
            return "morax://%s@%s:%d".formatted(name, rpcAddress.host(), rpcAddress.port());
        } else {
            return "morax-client://%s".formatted(name);
        }
    }
}
