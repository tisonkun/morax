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

package org.tisonkun.morax.io;

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
