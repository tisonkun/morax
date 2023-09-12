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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.bookie.storage.EntryLogIds;
import org.tisonkun.morax.bookie.storage.LedgerDirs;
import org.tisonkun.morax.proto.bookie.Entry;
import org.tisonkun.morax.proto.config.MoraxBookieServerConfig;

@Slf4j
public class Bookie {
    private final LedgerDirs ledgerDirs;
    private final ConcurrentMap<Long, Ledger> ledgers;

    public Bookie(MoraxBookieServerConfig serverConfig) {
        this.ledgerDirs = new LedgerDirs(serverConfig.getLedgerDirs());
        this.ledgers = new ConcurrentHashMap<>();
    }

    public void addEntry(Entry entry) throws IOException {
        final Ledger ledger = findLedger(entry.getLedgerId());
        ledger.addEntry(entry);
    }

    public Entry readEntry(long ledgerId, long entryId) throws IOException {
        final Ledger ledger = findLedger(ledgerId);
        return ledger.readEntry(entryId);
    }

    private Ledger findLedger(long ledgerId) {
        return ledgers.computeIfAbsent(ledgerId, ignore -> {
            final Path ledgerDir = ledgerDirs.getLedgerDir(ledgerId).toPath();
            final EntryLogIds logIds = new EntryLogIds(ledgerDirs);
            final Executor writeExecutor =
                    Executors.newSingleThreadExecutor(new DefaultThreadFactory("EntryLoggerWrite"));
            return new Ledger(ledgerId, ledgerDir, logIds, writeExecutor);
        });
    }
}
