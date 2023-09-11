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

import io.netty.buffer.ByteBuf;

public interface Entry {
    /**
     * @return the ledger ID of this entry.
     */
    long getLedgerId();

    /**
     * Non-negative results denote normal entries.
     *
     * @return the entry ID of this entry.
     */
    long getEntryId();

    /**
     * @return last confirmed associated with this entry; {@code -1} if not applicable.
     */
    long getLastConfirmed();

    /**
     * @return entry payload; {@code null} if not applicable.
     */
    ByteBuf getPayload();

    ByteBuf toBytes();

    static Entry fromBytes(ByteBuf entry) {
        final ByteBuf payload = entry.duplicate();
        final long ledgerId = payload.readLong();
        final long entryId = payload.readLong();
        final long lastConfirmed = payload.readLong();
        return new DefaultEntry(ledgerId, entryId, lastConfirmed, payload);
    }
}
