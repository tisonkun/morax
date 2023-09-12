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
            final Executor writeExecutor = Executors.newSingleThreadExecutor(
                    new DefaultThreadFactory("EntryLoggerWrite"));
            return new Ledger(ledgerId, ledgerDir,logIds, writeExecutor);
        });
    }
}
