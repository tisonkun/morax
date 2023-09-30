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

package org.tisonkun.morax.proto.bookie;

import java.nio.ByteBuffer;

public record EntryId(long ledgerId, long entryId) {
    public byte[] toBytes() {
        final byte[] bytes = new byte[Long.BYTES + Long.BYTES];
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.putLong(ledgerId);
        buffer.putLong(entryId);
        return bytes;
    }
}
