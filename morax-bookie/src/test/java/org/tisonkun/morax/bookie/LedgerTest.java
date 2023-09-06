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
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LedgerTest {

    @Test
    void testAddAndGetEntry(@TempDir Path tempDir) {
        final File ledgerFile = tempDir.resolve("1.log").toFile();
        final Ledger ledger = new Ledger(1, ledgerFile);

        final Entry[] entries = new Entry[] {
            new Entry(1, 1, 1, Unpooled.copiedBuffer("testAddAndGetEntry-1", StandardCharsets.UTF_8)),
            new Entry(1, 2, 2, Unpooled.copiedBuffer("testAddAndGetEntry-2", StandardCharsets.UTF_8)),
        };

        ledger.addEntry(entries[0]);
        ledger.addEntry(entries[1]);

        assertThat(ledger.getEntry(entries[1].getEntryId())).isEqualTo(entries[1]);
        assertThat(ledger.getEntry(entries[0].getEntryId())).isEqualTo(entries[0]);
        assertThat(ledger.getEntry(entries[1].getEntryId())).isEqualTo(entries[1]);

        for (int i = 0; i < 10; i++) {
            final int idx = ((new Random().nextInt() % 2) + 2) % 2;
            assertThat(ledger.getEntry(entries[idx].getEntryId())).isEqualTo(entries[idx]);
        }
    }
}
