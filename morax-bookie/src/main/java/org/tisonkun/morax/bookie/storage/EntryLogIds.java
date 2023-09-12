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

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Generate unique entry log ids.
 */
@Slf4j
public class EntryLogIds {
    private final LedgerDirs ledgerDirs;

    private int nextId;
    private int maxId;

    public EntryLogIds(LedgerDirs ledgerDirs) {
        this.ledgerDirs = ledgerDirs;
    }

    /**
     * Get the next available entry log ID. The log ID is unique with a single Bookie instance.
     */
    public int nextId() {
        while (true) {
            synchronized (this) {
                int current = nextId;
                nextId += 1;
                if (nextId < maxId) {
                    return current;
                }
                findLargestGap();
            }
        }
    }

    private void findLargestGap() {
        final Instant start = Instant.now();
        final List<File> dirs = ledgerDirs.getAllDirs();
        final List<Integer> currentLogIds = new ArrayList<>();

        for (File dir : dirs) {
            currentLogIds.addAll(LedgerDirs.findLogIds(dir));
        }
        final Pair<Integer, Integer> largestGap = LedgerDirs.findLargestGap(currentLogIds);
        nextId = largestGap.getLeft();
        maxId = largestGap.getRight();

        final long durationMs = Duration.between(start, Instant.now()).toMillis();
        log.atInfo()
                .addKeyValue("dirs", dirs)
                .addKeyValue("nextId", nextId)
                .addKeyValue("maxId", maxId)
                .addKeyValue("durationMs", durationMs)
                .log("event={}", StorageEvent.ENTRY_LOG_IDS_CANDIDATES_SELECTED.toString());
    }
}
