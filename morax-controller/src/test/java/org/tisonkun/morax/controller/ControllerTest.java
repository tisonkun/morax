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
    @Test
    void testRegisterService(@TempDir Path tempDir) throws Exception {
        final List<File> storageDir = Collections.singletonList(tempDir.toFile());
        final ControllerServerConfig config =
                ControllerServerConfig.builder().raftStorageDir(storageDir).build();
        final Controller controller = new Controller(config);
        try {
            controller.startUp();

            final var reply0 =
                    controller.listServices(ListBookiesRequest.newBuilder().build());
            assertThat(reply0).isEqualTo(ListBookiesReply.newBuilder().build());

            final ServiceProto serviceProto =
                    ServiceProto.newBuilder().setTarget("localhost:8080").build();

            final var reply1 = controller.registerService(
                    RegisterBookieRequest.newBuilder().setService(serviceProto).build());
            assertThat(reply1).isEqualTo(RegisterBookieReply.newBuilder().build());

            final var reply2 =
                    controller.listServices(ListBookiesRequest.newBuilder().build());
            assertThat(reply2)
                    .isEqualTo(ListBookiesReply.newBuilder()
                            .addService(serviceProto)
                            .build());
        } finally {
            controller.shutDown();
        }
    }
}
