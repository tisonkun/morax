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

import com.google.common.util.concurrent.AbstractIdleService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.File;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.proto.config.MoraxBookieServerConfig;

@Slf4j
public class BookieServer extends AbstractIdleService {
    private final Server server;

    public BookieServer(MoraxBookieServerConfig config) {
        this.server = ServerBuilder.forPort(10594)
                .addService(new BookieService(config))
                .build();
    }

    @Override
    protected void startUp() throws Exception {
        this.server.start();
        log.info("BookieServer started.");
    }

    @Override
    protected void shutDown() throws Exception {
        this.server.shutdown().awaitTermination();
        log.info("BookieServer terminated.");
    }

    public static void main(String[] args) throws Exception {
        final MoraxBookieServerConfig config = MoraxBookieServerConfig.builder()
                .ledgerDirs(List.of(new File("/tmp/morax-bookie")))
                .build();
        final BookieServer bookieServer = new BookieServer(config);
        bookieServer.startUp();
        bookieServer.shutDown();
    }
}
