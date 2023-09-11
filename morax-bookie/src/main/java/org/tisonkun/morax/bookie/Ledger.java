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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.proto.bookie.Entry;

@Slf4j
public class Ledger {
    // TODO(*) replace with a (RocksDB based) index engine.
    private final Map<Long, Long> positions = new HashMap<>();
    private final FastThreadLocal<ByteBuf> sizeBuf = new FastThreadLocal<>() {
        @Override
        protected ByteBuf initialValue() {
            return Unpooled.buffer(Integer.BYTES, Integer.BYTES);
        }
    };

    @Getter
    private final long ledgerId;

    // TODO(*) One file per ledger or mixed?
    private final File ledgerFile;

    private volatile long position = 0;

    public Ledger(long ledgerId, File ledgerFile) {
        this.ledgerId = ledgerId;
        this.ledgerFile = ledgerFile;
    }

    /**
     * Add an entry to the storage.
     */
    public void addEntry(Entry entry) {
        final Set<OpenOption> options = Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try (final FileChannel channel = FileChannel.open(this.ledgerFile.toPath(), options)) {
            channel.position(this.position);

            final ByteBuf bytes = entry.toBytes().duplicate();
            final int bytesLen = bytes.readableBytes();

            final ByteBuf sizeBuf = this.sizeBuf.get();
            sizeBuf.clear().writeInt(bytesLen).readBytes(channel, Integer.BYTES);

            bytes.readBytes(channel, bytesLen);
            this.positions.put(entry.getEntryId(), this.position);
            this.position = channel.position();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Read an entry from storage.
     */
    public Entry getEntry(long entryId) {
        if (!this.positions.containsKey(entryId)) {
            return null;
        }

        final long position = this.positions.get(entryId);
        try (final FileChannel channel = FileChannel.open(this.ledgerFile.toPath(), StandardOpenOption.READ)) {
            final ByteBuf sizeBuf = this.sizeBuf.get();
            sizeBuf.clear().writeBytes(channel, position, Integer.BYTES);

            final int bytesLen = sizeBuf.readInt();
            final ByteBuf entry = Unpooled.buffer(bytesLen);
            entry.writeBytes(channel, position + Integer.BYTES, bytesLen);
            return Entry.fromBytes(entry);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
