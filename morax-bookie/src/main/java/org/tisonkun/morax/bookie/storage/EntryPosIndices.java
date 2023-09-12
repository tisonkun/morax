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
import com.google.common.base.Preconditions;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.tuple.Pair;
import org.tisonkun.morax.proto.bookie.EntryLocation;

// TODO(*): replace with a (RocksDB-based) index engine.
public class EntryPosIndices {
    /**
     * (ledgerId, EntryId) -> (logId, position)
     */
    private final ConcurrentMap<Pair<Long, Long>, EntryLocation> positions = new ConcurrentHashMap<>();

    public void addPosition(long ledgerId, long entryId, int logId, long position) {
        final Pair<Long, Long> key = Pair.of(ledgerId, entryId);
        final EntryLocation value = new EntryLocation(logId, position);
        final EntryLocation prev = positions.putIfAbsent(key, value);
        Preconditions.checkState(
                prev == null,
                exMsg("Conflict position").kv("key", key).kv("value", value).toString());
    }

    public EntryLocation findPosition(long ledgerId, long entryId) {
        final Pair<Long, Long> key = Pair.of(ledgerId, entryId);
        return positions.get(key);
    }
}
