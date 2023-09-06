package org.tisonkun.morax.bookie;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class EntryTest {
    @Test
    void testCodec() {
        final var entry = new Entry(1, 1, 1, Unpooled.copiedBuffer("EntryTest", StandardCharsets.UTF_8));
        final var result = Entry.of(entry.toBytes());
        assertThat(result).isEqualTo(entry);
    }
}