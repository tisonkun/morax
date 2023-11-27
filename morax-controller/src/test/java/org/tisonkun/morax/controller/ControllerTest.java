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

import static org.assertj.core.api.Assertions.assertThat;
import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.tisonkun.morax.proto.config.ControllerServerConfig;
import org.tisonkun.morax.proto.controller.ListBookiesReply;
import org.tisonkun.morax.proto.controller.ListBookiesRequest;
import org.tisonkun.morax.proto.controller.RegisterBookieReply;
import org.tisonkun.morax.proto.controller.RegisterBookieRequest;
import org.tisonkun.morax.proto.controller.ServiceProto;

class ControllerTest {
    private final Duration timing = Duration.ofSeconds(10);

    @Test
    void testRegisterService(@TempDir Path tempDir) throws Exception {
        final List<File> storageDir = Collections.singletonList(tempDir.toFile());
        final ControllerServerConfig config =
                ControllerServerConfig.builder().raftStorageDir(storageDir).build();
        final Controller controller = new Controller(config);
        try {
            controller.startAsync();
            controller.awaitRunning(timing);

            // empty
            {
                final var request = ListBookiesRequest.newBuilder().build();
                final var reply = controller.listServices(request);
                final var expectedReply = ListBookiesReply.newBuilder().build();
                assertThat(reply).isEqualTo(expectedReply);
            }

            final ServiceProto service =
                    ServiceProto.newBuilder().setTarget("localhost:8080").build();

            // register
            {
                final var request =
                        RegisterBookieRequest.newBuilder().setService(service).build();
                final var reply = controller.registerService(request);
                final var expectedReply = RegisterBookieReply.newBuilder().build();
                assertThat(reply).isEqualTo(expectedReply);
            }

            // registered
            {
                final var request = ListBookiesRequest.newBuilder().build();
                final var reply = controller.listServices(request);
                final var expectedReply =
                        ListBookiesReply.newBuilder().addService(service).build();
                assertThat(reply).isEqualTo(expectedReply);
            }

            // persisted
            {
                final var request = ListBookiesRequest.newBuilder().build();
                final var reply = controller.listServices(request);
                final var expectedReply =
                        ListBookiesReply.newBuilder().addService(service).build();
                assertThat(reply).isEqualTo(expectedReply);
            }
        } finally {
            controller.stopAsync();
            controller.awaitTerminated(timing);
        }
    }
}
