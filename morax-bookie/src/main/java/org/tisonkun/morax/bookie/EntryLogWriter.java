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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.morax.proto.exception.ExceptionUtils;

/**
 * This class writes data to one entry log.
 */
@Slf4j
public class EntryLogWriter implements AutoCloseable {
    private final Object bufferLock = new Object();
    private final List<CompletableFuture<Void>> outstandingWrites = new ArrayList<>();

    private final FileChannel channel;
    private final int logId;
    private final Path logFile;
    private final Executor writeExecutor;

    private ByteBuf byteBuf;
    private long offset;

    public EntryLogWriter(int logId, Path logFile, Executor writeExecutor) throws IOException {
        this.logId = logId;
        this.logFile = logFile;
        this.writeExecutor = writeExecutor;

        final Set<OpenOption> options = Set.of(StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        this.channel = FileChannel.open(logFile, options);

        this.byteBuf = acquireByteBuf();
        this.offset = 0;
    }

    /**
     * Return the ID of the log being written.
     */
    public int logId() {
        return logId;
    }

    /**
     * The current offset within the log at which the next call to {@link #writeDelimited} will start writing.
     */
    public long position() {
        synchronized (bufferLock) {
            return offset + (byteBuf != null ? byteBuf.readableBytes() : 0);
        }
    }

    /**
     * Write a delimited buffer the log. The size of the buffer is first written and then the buffer itself.
     * <p>
     * Note that the returned offset is for the buffer itself, not the size. So, if a buffer is written at the
     * start of the file, the returned offset will be 4, not 0.
     *
     * @return the offset of the buffer within the file.
     */
    public long writeDelimited(ByteBuf buf) throws IOException {
        synchronized (bufferLock) {
            if (byteBuf.maxWritableBytes() < serializedSize(buf)) {
                flushBuffer();
            }

            final int readable = buf.readableBytes();
            final long beforePosition = position();
            final long bufferPosition = beforePosition + Integer.BYTES;
            if (bufferPosition < 0) {
                throw new IOException(exMsg("Position exceeded")
                        .kv("logFile", logFile)
                        .kv("writeSize", readable)
                        .kv("beforePosition", beforePosition)
                        .kv("afterPosition", bufferPosition)
                        .toString());
            }
            byteBuf.writeInt(readable);
            byteBuf.writeBytes(buf);
            return bufferPosition;
        }
    }

    /**
     * Flush all buffered writes to disk. This call must ensure that the bytes are actually on
     * disk before returning.
     */
    public void flush() throws IOException {
        flushBuffer();
        waitOutstandingWrites();
        this.channel.force(true);
    }

    // TODO(*): should use a buffer pool to allocate and reuse buffers.
    private ByteBuf acquireByteBuf() {
        return ByteBufAllocator.DEFAULT.buffer();
    }

    private void writeBuffer(ByteBuf buf, int bytesToWrite, long offsetToWrite) throws IOException {
        if (bytesToWrite <= 0) {
            return;
        }

        final int index = buf.readerIndex();
        final int written = buf.getBytes(index, channel, offsetToWrite, bytesToWrite);
        if (written != bytesToWrite) {
            throw new IOException(exMsg("Incomplete write")
                    .kv("logFile", logFile)
                    .kv("index", index)
                    .kv("writeSize", bytesToWrite)
                    .kv("offset", offsetToWrite)
                    .kv("bytesWritten", written)
                    .toString());
        }
    }

    private void flushBuffer() {
        synchronized (bufferLock) {
            if (byteBuf != null && byteBuf.readableBytes() > 0) {
                final int bytesToWrite = byteBuf.readableBytes();

                final ByteBuf bufferToFlush = byteBuf;
                byteBuf = null;

                final long offsetToWrite = offset;
                offset += bytesToWrite;

                addOutstandingWrite(CompletableFuture.runAsync(() -> {
                    try {
                        writeBuffer(bufferToFlush, bytesToWrite, offsetToWrite);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }, writeExecutor));

                // must acquire after triggering the write
                // otherwise it could try to acquire a buffer without kicking off
                // a subroutine that will free another
                byteBuf = acquireByteBuf();
            }
        }
    }

    private void addOutstandingWrite(CompletableFuture<Void> future) {
        synchronized (outstandingWrites) {
            outstandingWrites.add(future);

            // clear out completed futures
            final var iter = outstandingWrites.iterator();
            while (iter.hasNext()) {
                final var f = iter.next();
                if (f.isDone()) {
                    waitOutstandingWrite(f);
                    iter.remove();
                } else {
                    break;
                }
            }
        }
    }

    private void waitOutstandingWrites() {
        synchronized (outstandingWrites) {
            final var iter = outstandingWrites.iterator();
            while (iter.hasNext()) {
                final var f = iter.next();
                waitOutstandingWrite(f);
                iter.remove();
            }
        }
    }

    private static void waitOutstandingWrite(CompletableFuture<Void> future) {
        try {
            future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtils.asUncheckedIOException(e);
        } catch (ExecutionException e) {
            final Throwable stripped = ExceptionUtils.stripException(e, ExecutionException.class);
            throw ExceptionUtils.asUncheckedIOException(stripped);
        } catch (Throwable t) {
            throw ExceptionUtils.asUncheckedIOException(t);
        }
    }

    /**
     * Close any held resources.
     */
    @Override
    public void close() throws IOException {
        synchronized (bufferLock) {
            if (byteBuf != null && byteBuf.readableBytes() > 0) {
                flush();
            }
        }

        this.channel.close();
    }

    public static long serializedSize(ByteBuf buf) {
        return buf.readableBytes() + Integer.BYTES;
    }
}
