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

package org.tisonkun.morax.controller;

import com.google.common.util.concurrent.AbstractIdleService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.proto.config.MoraxControllerServerConfig;

@Slf4j
public class ControllerServer extends AbstractIdleService {
    private final Server server;
    private final Controller controller;

    public ControllerServer(MoraxControllerServerConfig config) throws IOException {
        this.controller = new Controller(config);
        this.server = ServerBuilder.forPort(config.getPort())
                .addService(new ControllerService(this.controller))
                .build();
    }

    @Override
    protected void startUp() throws Exception {
        this.controller.startUp();
        this.server.start();
        log.info("ControllerServer started.");
    }

    @Override
    protected void shutDown() throws Exception {
        this.controller.shutDown();
        this.server.shutdown().awaitTermination();
        log.info("ControllerServer terminated.");
    }

    public static void main(String[] args) throws Exception {
        final MoraxControllerServerConfig config =
                MoraxControllerServerConfig.builder().build();
        final ControllerServer bookieServer = new ControllerServer(config);
        bookieServer.startUp();
        bookieServer.shutDown();
    }
}
