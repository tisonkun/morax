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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * This class manages ledger directories used by the bookie.
 */
@Slf4j
public class LedgerDirs {
    public static final Pattern FILE_PATTERN = Pattern.compile("^([0-9a-fA-F]+)\\.log$");

    public static List<Integer> findLogIds(File dir) {
        if (dir.exists() && dir.isDirectory()) {
            final File[] files = dir.listFiles();
            if (files != null) {
                final List<Integer> logIds = new ArrayList<>();
                for (File file : files) {
                    final Matcher m = FILE_PATTERN.matcher(file.getName());
                    if (m.matches()) {
                        int logId = Integer.parseUnsignedInt(m.group(1), 16);
                        logIds.add(logId);
                    }
                }
                return logIds;
            }
        }
        return Collections.emptyList();
    }

    /**
     * Find the largest contiguous gap between integers in a passed list.
     *
     * @param logIds log IDs of the current log files.
     * @return a pair of {@code nextId} and {@code maxId}.
     */
    public static Pair<Integer, Integer> findLargestGap(List<Integer> logIds) {
        if (logIds.isEmpty()) {
            return Pair.of(0, Integer.MAX_VALUE);
        }

        Collections.sort(logIds);
        int nextId = 0;
        int maxId = logIds.get(0);
        int maxGap = maxId - nextId;
        for (int i = 0; i < logIds.size(); i++) {
            final int gapStart = logIds.get(i) + 1;
            final int gapEnd;
            if (i + 1 < logIds.size()) {
                gapEnd = logIds.get(i + 1);
            } else {
                gapEnd = Integer.MAX_VALUE;
            }
            final int gapSize = gapEnd - gapStart;
            if (gapSize > maxGap) {
                maxGap = gapSize;
                nextId = gapStart;
                maxId = gapEnd;
            }
        }
        return Pair.of(nextId, maxId);
    }

    private final List<File> dirs;

    public LedgerDirs(List<File> dirs) {
        this.dirs = dirs;
    }

    public File getLedgerDir(long ledgerId) {
        final int index = Math.floorMod(ledgerId, dirs.size());
        return dirs.get(index);
    }

    public List<File> getAllDirs() {
        return dirs;
    }
}
