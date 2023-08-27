/*
 * Copyright (c) 2023 tison <wander4096@gmail.com>
 * All rights reserved.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.tisonkun.nymph.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import io.netty.util.concurrent.FastThreadLocalThread;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.tisonkun.nymph.bookie.callback.WriteCallback;

@Slf4j
public class Journal extends FastThreadLocalThread {
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final BlockingQueue<QueueEntry> entryQueue = new LinkedBlockingQueue<>();
    private final Path journalPath;

    public Journal() throws IOException {
        super("BookieJournal");
        this.journalPath = Files.createTempFile(null, null);
    }

    public void appendEntry(ByteBuf entry, WriteCallback callback) {
        long ledgerId = entry.getLong(entry.readerIndex());
        long entryId = entry.getLong(entry.readerIndex() + 8);

        // retain entry until written
        entry.retain();
        entryQueue.add(QueueEntry.create(ledgerId, entryId, entry, callback));
    }

    @SneakyThrows
    public void scan() {
        try (final FileChannel journalChannel = FileChannel.open(journalPath, StandardOpenOption.READ)) {
            final ByteBuffer length = ByteBuffer.allocate(4);
            while (true) {
                length.clear();
                journalChannel.read(length);
                if (length.remaining() != 0) {
                    break;
                }
                length.flip();
                final int len = length.getInt();
                System.out.println("len = " + len);
                final ByteBuffer entry = ByteBuffer.allocate(len);
                journalChannel.read(entry);
                entry.flip();
                System.out.println("entry = " + entry);
            }
        }
    }

    private static class QueueEntry implements Runnable {
        private static final Recycler<QueueEntry> RECYCLER = new Recycler<>() {
            @Override
            protected QueueEntry newObject(Handle<QueueEntry> handle) {
                return new QueueEntry(handle);
            }
        };

        private final Recycler.Handle<QueueEntry> handle;
        private long ledgerId;
        private long entryId;
        private ByteBuf entry;
        private WriteCallback callback;

        private static QueueEntry create(long ledgerId, long entryId, ByteBuf entry, WriteCallback callback) {
            final QueueEntry qe = RECYCLER.get();
            qe.ledgerId = ledgerId;
            qe.entryId = entryId;
            qe.entry = entry;
            qe.callback = callback;
            return qe;
        }

        private QueueEntry(Recycler.Handle<QueueEntry> handle) {
            this.handle = handle;
        }

        @Override
        public void run() {
            callback.process(0, ledgerId, entryId, null);
            recycle();
        }

        private void recycle() {
            entry = null;
            callback = null;
            handle.recycle(this);
        }
    }

    public void shutdown() {
        if (this.running.compareAndSet(true, false)) {
            // ensure exactly once shutdown
            log.info("Shutting down the Journal thread.");
            this.interrupt();
        }
    }

    @Override
    public void run() {
        try {
            doRun();
        } catch (InterruptedException e) {
            log.info("Journal main loop exits when shutting down.");
            Thread.currentThread().interrupt();
        } catch (UncheckedIOException | IOException e) {
            log.error("Journal thread encounters IO exception.", e);
        }

        log.info("Journal main loop exited.");
    }

    private void doRun() throws IOException, InterruptedException {
        while (running.get()) {
            final ByteBuf length = Unpooled.buffer(4);
            final QueueEntry qe = entryQueue.take();
            try (final FileChannel journalChannel = FileChannel.open(journalPath, StandardOpenOption.APPEND)) {
                length.clear();
                length.writeInt(qe.entry.readableBytes());
                journalChannel.write(length.nioBuffer());
                journalChannel.write(qe.entry.nioBuffer());
            }
            qe.run();
        }
    }

    public static void main(String[] args) throws Exception {
        final Journal journal = new Journal();
        journal.start();
        final ByteBuf buf = Unpooled.buffer();
        buf.writeLong(1);
        buf.writeLong(1);
        buf.writeBytes("Nymph".getBytes());
        final CountDownLatch latch = new CountDownLatch(1);
        journal.appendEntry(buf, (rc, ledgerId, entryId, bookieId) -> {
            System.out.printf(
                    "Callback with rc=%d, ledgerId=%d, entryId=%d, bookieId=%s\n", rc, ledgerId, entryId, bookieId);
            latch.countDown();
        });
        latch.await();
        journal.scan();
        journal.shutdown();
    }
}
