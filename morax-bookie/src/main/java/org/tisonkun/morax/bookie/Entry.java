package org.tisonkun.morax.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import lombok.Data;

@Data
public class Entry {
    private final long ledgerId;
    private final long entryId;
    private final long lastAddConfirmed;
    private final ByteBuf payload;

    public static Entry of(ByteBuf entry) {
        final ByteBuf payload = entry.duplicate();
        final long ledgerId = payload.readLong();
        final long entryId = payload.readLong();
        final long lastAddConfirmed = payload.readLong();
        return new Entry(ledgerId, entryId, lastAddConfirmed, payload);
    }

    public ByteBuf toBytes() {
        final ByteBuf result = Unpooled.buffer(8 + 8 + 8 + payload.readableBytes());
        result.writeLong(ledgerId);
        result.writeLong(entryId);
        result.writeLong(lastAddConfirmed);
        result.writeBytes(payload, payload.readerIndex(), payload.readableBytes());
        return result;
    }

    @Override
    public String toString() {
        return "Entry{" +
                "ledgerId=" + ledgerId +
                ", entryId=" + entryId +
                ", lastAddConfirmed=" + lastAddConfirmed +
                ", payload=" + payload.toString(StandardCharsets.UTF_8) +
                '}';
    }
}
