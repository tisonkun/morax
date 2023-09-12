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

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.tisonkun.morax.proto.io.BufferUtils;

public class EntryProtoEntryAdaptor implements Entry {
    private final EntryProto entryProto;

    public EntryProtoEntryAdaptor(EntryProto entryProto) {
        this.entryProto = entryProto;
    }

    @Override
    public long getLedgerId() {
        return entryProto.getLedgerId();
    }

    @Override
    public long getEntryId() {
        return entryProto.getEntryId();
    }

    @Override
    public long getLastConfirmed() {
        return entryProto.getLastConfirmed();
    }

    @Override
    public ByteBuf getPayload() {
        return BufferUtils.byteStringToByteBuf(entryProto.getPayload());
    }

    @Override
    public int serializedSize() {
        return Long.BYTES // ledgerId
                + Long.BYTES // entryId
                + Long.BYTES // lastConfirmed
                + entryProto.getPayload().size(); // payload
    }

    @Override
    public void writeToBytes(ByteBuf byteBuf) {
        byteBuf.writeLong(getLedgerId());
        byteBuf.writeLong(getEntryId());
        byteBuf.writeLong(getLastConfirmed());
        byteBuf.writeBytes(entryProto.getPayload().asReadOnlyByteBuffer());
    }

    @Override
    public EntryProto toEntryProto() {
        return entryProto;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getLedgerId(), getEntryId());
    }

    @Override
    public String toString() {
        return "EntryProtoEntryAdaptor{" + "entryProto=" + entryProto + '}';
    }
}
