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

package org.tisonkun.morax.proto.config;

import java.io.File;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Builder
@Jacksonized
public class ControllerServerConfig {
    @Builder.Default
    private int serverPort = 10386;

    @Builder.Default
    private String raftPeerId = "n0";

    @Builder.Default
    private List<File> raftStorageDir = Collections.singletonList(new File("/tmp/morax/controller/raft-server"));

    @Builder.Default
    private RaftGroupConfig raftGroup = new RaftGroupConfig(List.of(new RaftPeerConfig("n0", "127.0.0.1:12386")));
}
