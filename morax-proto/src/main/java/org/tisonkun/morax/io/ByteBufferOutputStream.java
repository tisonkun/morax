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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Provide a zero-copy way to convert data in ByteArrayOutputStream to ByteBuffer
 */
public class ByteBufferOutputStream extends ByteArrayOutputStream {
    private volatile boolean closed = false;

    public ByteBufferOutputStream() {
        this(32);
    }

    public ByteBufferOutputStream(int capacity) {
        super(capacity);
    }

    @Override
    public synchronized void write(int b) {
        if (closed) {
            throw new IllegalStateException("cannot write to a closed ByteBufferOutputStream");
        }
        super.write(b);
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
        if (closed) {
            throw new IllegalStateException("cannot write to a closed ByteBufferOutputStream");
        }
        super.write(b, off, len);
    }

    @Override
    public synchronized void reset() {
        if (closed) {
            throw new IllegalStateException("cannot write to a closed ByteBufferOutputStream");
        }
        super.reset();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            super.close();
        }
    }

    public ByteBuffer toByteBuffer() {
        if (!closed) {
            throw new IllegalStateException(
                    "can only call toByteBuffer() after ByteBufferOutputStream has been closed");
        }
        return ByteBuffer.wrap(buf, 0, count);
    }
}
