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

import java.nio.file.Path;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.tisonkun.morax.proto.bookie.EntryId;
import org.tisonkun.morax.proto.bookie.EntryLocation;
import org.tisonkun.morax.proto.exception.ExceptionUtils;

public class EntryPosIndices {
    // TODO(*) support checkpoint
    private final RocksDB db;

    public EntryPosIndices(Path dbPath) {
        try {
            this.db = RocksDB.open(dbPath.toString());
        } catch (RocksDBException e) {
            throw ExceptionUtils.asUncheckedIOException(e);
        }
    }

    public void addPosition(long ledgerId, long entryId, int logId, long position) {
        final EntryId key = new EntryId(ledgerId, entryId);
        final EntryLocation value = new EntryLocation(logId, position);
        try {
            db.put(key.toBytes(), value.toBytes());
        } catch (RocksDBException e) {
            throw ExceptionUtils.asUncheckedIOException(e);
        }
    }

    public EntryLocation findPosition(long ledgerId, long entryId) {
        final EntryId key = new EntryId(ledgerId, entryId);
        final byte[] value;
        try {
            value = db.get(key.toBytes());
        } catch (RocksDBException e) {
            throw ExceptionUtils.asUncheckedIOException(e);
        }
        return EntryLocation.fromBytes(value);
    }
}
