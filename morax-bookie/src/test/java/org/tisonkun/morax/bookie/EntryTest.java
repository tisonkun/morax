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

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class EntryTest {
    @Test
    void testCodec() {
        final var entry = new Entry(1, 1, 1, Unpooled.copiedBuffer("EntryTest", StandardCharsets.UTF_8));
        final var result = Entry.of(entry.toBytes());
        assertThat(result).isEqualTo(entry);
    }
}
