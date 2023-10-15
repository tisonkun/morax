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

package org.tisonkun.morax.bookie.storage;

import static org.tisonkun.morax.proto.exception.ExceptionMessageBuilder.exMsg;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.proto.bookie.Entry;
import org.tisonkun.morax.proto.bookie.EntryLocation;
import org.tisonkun.morax.proto.exception.ExceptionUtils;

@Slf4j
public class EntryLogger {
    private static final String LOG_FILE_SUFFIX = ".log";

    private final Cache<Integer, EntryLogReader> entryLogReaderCache = CacheBuilder.newBuilder()
            .concurrencyLevel(1) // important to avoid too aggressive eviction
            .build();
    private final Path entryLogDir;
    private final EntryLogIds logIds;
    private final Executor writeExecutor;
    private EntryLogWriter entryLogWriter;

    public EntryLogger(Path entryLogDir, EntryLogIds logIds, Executor writeExecutor) {
        this.entryLogDir = entryLogDir;
        this.logIds = logIds;
        this.writeExecutor = writeExecutor;
    }

    public EntryLocation addEntry(Entry entry) throws IOException {
        if (entryLogWriter == null) {
            final int logId = logIds.nextId();
            entryLogWriter = new EntryLogWriter(logId, entryLogDir.resolve(logFileName(logId)), writeExecutor);
            log.atInfo().addKeyValue("newLogId", logId).log("event={}", StorageEvent.LOG_ROLL.toString());
        }
        final int logId = entryLogWriter.logId();
        final long offset = entryLogWriter.writeDelimitedEntry(entry.toBytes());
        // TODO(*) move to background thread and change the return value of this method as future.
        entryLogWriter.flush();
        return new EntryLocation(logId, offset);
    }

    public Entry readEntry(long ledgerId, long entryId, EntryLocation location) throws IOException {
        if (location == null) {
            return null;
        }

        final int logId = location.logId();
        final EntryLogReader entryLogReader;
        try {
            entryLogReader = entryLogReaderCache.get(logId, () -> {
                final Path logFile = entryLogDir.resolve(logFileName(logId));
                return new EntryLogReader(logId, logFile);
            });
        } catch (ExecutionException e) {
            final Throwable t = ExceptionUtils.stripException(e, ExecutionException.class);
            if (t instanceof IOException ioe) {
                throw ioe;
            } else {
                final String message = exMsg("Error loading reader in cache")
                        .kv("logId", logId)
                        .toString();
                throw new IOException(message, t);
            }
        }

        // It is possible though unlikely, that the cache has already cleaned up this cache entry
        // during the get operation. This is more likely to happen when there is great demand
        // for many separate readers in a low memory environment.
        if (entryLogReader.isClosed()) {
            throw new IOException(
                    exMsg("Cached reader already closed").kv("logId", logId).toString());
        }

        final Entry entry = Entry.fromBytes(entryLogReader.readEntryAt(location.position()));
        // sanity check entry
        final long thisLedgerId = entry.getLedgerId();
        final long thisEntryId = entry.getEntryId();
        if (thisLedgerId != ledgerId || thisEntryId != entryId) {
            throw new IOException(exMsg("Bad location")
                    .kv("location", location)
                    .kv("expectedLedger", ledgerId)
                    .kv("expectedEntry", entryId)
                    .kv("foundLedger", thisLedgerId)
                    .kv("foundEntry", thisEntryId)
                    .toString());
        }
        return entry;
    }

    public static String logFileName(int logId) {
        return Long.toHexString(logId) + LOG_FILE_SUFFIX;
    }
}
