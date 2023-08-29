package org.tisonkun.nymph.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Reads data from a ByteBuffer.
 */
public class ByteBufferInputStream extends InputStream {
    private volatile ByteBuffer buffer;

    public ByteBufferInputStream(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() throws IOException {
        if (buffer == null || buffer.remaining() == 0) {
            cleanUp();
            return -1;
        } else {
            return buffer.get() & 0xFF;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (buffer == null || buffer.remaining() == 0) {
            cleanUp();
            return -1;
        } else {
            final int amountToGet = Math.min(buffer.remaining(), len);
            buffer.get(b, off, amountToGet);
            return amountToGet;
        }
    }

    @Override
    public long skip(long n) {
        if (buffer != null) {
            final int amountToSkip = (int) Math.min(n, buffer.remaining());
            buffer.position(buffer.position() + amountToSkip);
            if (buffer.remaining() == 0) {
                cleanUp();
            }
            return amountToSkip;
        } else {
            return 0;
        }
    }

    private void cleanUp() {
        if (buffer != null) {
            buffer = null;
        }
    }
}
