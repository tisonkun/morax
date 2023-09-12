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

import static org.tisonkun.morax.proto.exception.ExceptionMessageBuilder.exMsg;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import lombok.Getter;

public class EntryLogReader implements AutoCloseable {
    private final int logId;
    private final Path logFile;
    private final FileChannel channel;

    @Getter
    private volatile boolean closed;

    public EntryLogReader(int logId, Path logFile) throws IOException {
        this.logId = logId;
        this.logFile = logFile;

        final Set<OpenOption> options = Set.of(StandardOpenOption.READ);
        this.channel = FileChannel.open(logFile, options);
    }

    /**
     * Return the ID of the log being read from.
     */
    public int logId() {
        return logId;
    }

    /**
     * Read an integer at a given offset.
     *
     * @param offset the offset to read from.
     * @return the integer at that offset.
     */
    public int readIntAt(long offset) throws IOException {
        assertValidOffset(offset);
        final ByteBuf intBuf = readBufferAt(offset, Integer.BYTES);
        try {
            return intBuf.getInt(0);
        } finally {
            ReferenceCountUtil.release(intBuf);
        }
    }

    /**
     * Read a buffer from the file. It is the responsibility of the caller to release
     * the returned buffer.
     *
     * @param offset the offset to read at
     * @param size   the number of bytes to read
     * @return a {@link ByteBuf} that the caller must release.
     */
    public ByteBuf readBufferAt(long offset, int size) throws IOException {
        final ByteBuf buf = acquireByteBuf(size);
        try {
            long cursor = offset;
            int remain = size;
            while (remain > 0) {
                int bytesRead = buf.writeBytes(channel, cursor, remain);
                remain -= bytesRead;
                cursor += bytesRead;
            }
        } catch (IOException e) {
            ReferenceCountUtil.release(buf);
            throw e;
        }
        return buf;
    }

    /**
     * Read an entry at a given offset.
     * <p>
     * The size of the entry must be at {@code offset - Integer.BYTES}. The payload of the entry starts at offset.
     * It is the responsibility of the caller to release the returned buffer.
     *
     * @param offset the offset at which to read the entry.
     * @return a {@link ByteBuf} that the caller must release.
     */
    public ByteBuf readEntryAt(long offset) throws IOException {
        assertValidOffset(offset);
        final long sizeOffset = offset - Integer.BYTES;
        if (sizeOffset < 0) {
            throw new IOException(exMsg("Invalid offset; buffer size missing")
                    .kv("logFile", logFile)
                    .kv("offset", offset)
                    .toString());
        }
        final int entrySize = readIntAt(sizeOffset);
        if (entrySize <= 0) {
            throw new IOException(exMsg("Invalid entry size")
                    .kv("logFile", logFile)
                    .kv("offset", offset)
                    .kv("readEntrySize", entrySize)
                    .toString());
        }
        return readBufferAt(offset, entrySize);
    }

    // TODO(*): should use a buffer pool to allocate and reuse buffers.
    private ByteBuf acquireByteBuf(int size) {
        return ByteBufAllocator.DEFAULT.buffer(size);
    }

    private static void assertValidOffset(long offset) {
        Preconditions.checkArgument(
                offset >= 0,
                exMsg("Offset cannot be negative").kv("offset", offset).toString());
    }

    @Override
    public void close() throws Exception {
        closed = true;
        channel.close();
    }
}
