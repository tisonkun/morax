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

package org.tisonkun.morax.controller;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.tisonkun.morax.proto.controller.ControllerRequestType;
import org.tisonkun.morax.proto.controller.ListBookiesRequest;
import org.tisonkun.morax.proto.controller.RegisterBookieRequest;
import org.tisonkun.morax.proto.io.BufferUtils;

public record RequestMessage(ControllerRequestType type, GeneratedMessageV3 message) implements Message {
    @Override
    public ByteString getContent() {
        final ByteBuf typeByteBuf = Unpooled.buffer().writeInt(type.getNumber());
        final ByteBuf msgByteBuf = BufferUtils.byteStringToByteBuf(message.toByteString());
        return BufferUtils.byteBufToByteString(Unpooled.wrappedBuffer(typeByteBuf, msgByteBuf));
    }

    public static RequestMessage fromBytes(ByteString bytes) throws InvalidProtocolBufferException {
        final ByteBuf mixedByteBuf = BufferUtils.byteStringToByteBuf(bytes);
        final ControllerRequestType type = ControllerRequestType.forNumber(mixedByteBuf.readInt());
        final GeneratedMessageV3 message =
                switch (type) {
                    case RegisterBookie -> RegisterBookieRequest.parseFrom(mixedByteBuf.nioBuffer());
                    case ListBookies -> ListBookiesRequest.parseFrom(mixedByteBuf.nioBuffer());
                    case UNRECOGNIZED -> throw new InvalidProtocolBufferException("unrecognized request type");
                    case null -> throw new InvalidProtocolBufferException(new NullPointerException("type"));
                };
        return new RequestMessage(type, message);
    }
}
