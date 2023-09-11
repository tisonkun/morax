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

package org.tisonkun.morax.proto.bookie;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class DefaultEntryTest {
    @Test
    void testCodec() {
        final ByteBuf payload = Unpooled.copiedBuffer("DefaultEntryTest", StandardCharsets.UTF_8);
        final Entry entry = new DefaultEntry(1, 1, 1, payload);
        final Entry result = Entry.fromBytes(entry.toBytes());
        assertThat(result).isEqualTo(entry);
    }
}