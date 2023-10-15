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

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.bookie.storage.EntryLogger;
import org.tisonkun.morax.bookie.storage.EntryPosIndices;
import org.tisonkun.morax.proto.bookie.Entry;
import org.tisonkun.morax.proto.bookie.EntryLocation;

@Slf4j
public class Ledger {
    private final long ledgerId;
    private final EntryLogger entryLogger;
    private final EntryPosIndices entryPosIndices;

    public Ledger(long ledgerId, EntryLogger entryLogger, EntryPosIndices entryPosIndices) {
        this.ledgerId = ledgerId;
        this.entryLogger = entryLogger;
        this.entryPosIndices = entryPosIndices;
    }

    public void addEntry(Entry entry) throws IOException {
        final EntryLocation location = entryLogger.addEntry(entry);
        entryPosIndices.addPosition(ledgerId, entry.getEntryId(), location.logId(), location.position());
    }

    public Entry readEntry(long entryId) throws IOException {
        final EntryLocation location = entryPosIndices.findPosition(ledgerId, entryId);
        return entryLogger.readEntry(ledgerId, entryId, location);
    }
}
