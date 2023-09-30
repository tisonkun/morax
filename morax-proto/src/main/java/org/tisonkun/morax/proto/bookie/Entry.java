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

import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.tisonkun.morax.proto.io.BufferUtils;

@Data
public final class Entry {
    /**
     * The ledger ID of this entry.
     */
    private final long ledgerId;

    /**
     * The entry ID of this entry; non-negative results denote normal entries.
     */
    private final long entryId;

    /**
     * Last confirmed associated with this entry; {@code -1} if not applicable.
     */
    private final long lastConfirmed;

    /**
     * Entry payload; {@code null} if not applicable.
     */
    private final ByteBuf payload;

    @EqualsAndHashCode.Exclude
    private transient volatile ByteBuf cachedBytes;

    /**
     * Serialize this entry to bytes.
     */
    public ByteBuf toBytes() {
        return cachedBytes();
    }

    /**
     * @return {@link EntryProto} that is logically identical to this entry.
     */
    public EntryProto toEntryProto() {
        return EntryProto.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setLastConfirmed(lastConfirmed)
                .setPayload(ByteString.copyFrom(payload.nioBuffer()))
                .build();
    }

    private ByteBuf cachedBytes() {
        if (cachedBytes != null) {
            return cachedBytes;
        }
        final ByteBuf result = Unpooled.buffer(8 + 8 + 8 + payload.readableBytes());
        result.writeLong(ledgerId);
        result.writeLong(entryId);
        result.writeLong(lastConfirmed);
        result.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
        cachedBytes = result;
        return result;
    }

    @Override
    public String toString() {
        return "Entry{" + "ledgerId="
                + ledgerId + ", entryId="
                + entryId + ", lastConfirmed="
                + lastConfirmed + ", payload="
                + payload.toString(StandardCharsets.UTF_8) + '}';
    }

    public static Entry fromBytes(ByteBuf entry) {
        final ByteBuf payload = entry.duplicate();
        final long ledgerId = payload.readLong();
        final long entryId = payload.readLong();
        final long lastConfirmed = payload.readLong();
        return new Entry(ledgerId, entryId, lastConfirmed, payload);
    }

    public static Entry fromProtos(EntryProto entryProto) {
        final long ledgerId = entryProto.getLedgerId();
        final long entryId = entryProto.getEntryId();
        final long lastConfirmed = entryProto.getLastConfirmed();
        final ByteBuf payload = BufferUtils.byteStringToByteBuf(entryProto.getPayload());
        return new Entry(ledgerId, entryId, lastConfirmed, payload);
    }
}
