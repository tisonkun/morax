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

package org.tisonkun.morax.proto.io;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import lombok.experimental.UtilityClass;

@UtilityClass
public class BufferUtils {
    public static ByteBuf byteStringToByteBuf(ByteString bytes) {
        final ByteBuffer byteBuffer = bytes.asReadOnlyByteBuffer();
        return Unpooled.wrappedBuffer(byteBuffer);
    }

    public static org.apache.ratis.thirdparty.com.google.protobuf.ByteString byteStringDoShade(ByteString bytes) {
        return org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations.unsafeWrap(
                bytes.asReadOnlyByteBuffer());
    }

    public static ByteString byteStringUndoShade(org.apache.ratis.thirdparty.com.google.protobuf.ByteString bytes) {
        return UnsafeByteOperations.unsafeWrap(bytes.asReadOnlyByteBuffer());
    }
}
