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

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.tisonkun.morax.proto.controller.ListBookiesReply;
import org.tisonkun.morax.proto.controller.ListBookiesRequest;
import org.tisonkun.morax.proto.controller.RegisterBookieReply;
import org.tisonkun.morax.proto.controller.RegisterBookieRequest;
import org.tisonkun.morax.proto.exception.ExceptionMessageBuilder;
import org.tisonkun.morax.proto.io.BufferUtils;

public class ControllerStateMachine extends BaseStateMachine {

    private final ControllerState state = new ControllerState();

    @Override
    public CompletableFuture<Message> query(Message request) {
        final RequestMessage message;
        if (request instanceof RequestMessage localMessage) {
            message = localMessage;
        } else {
            try {
                message = RequestMessage.fromBytes(request.getContent());
            } catch (InvalidProtocolBufferException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        if (message.message() instanceof ListBookiesRequest) {
            final ListBookiesReply.Builder reply = ListBookiesReply.newBuilder();
            reply.addAllService(state.listBookies());
            final ByteString content =
                    BufferUtils.byteStringDoShade(reply.build().toByteString());
            return CompletableFuture.completedFuture(Message.valueOf(content));
        } else {
            final String exMsg = ExceptionMessageBuilder.exMsg("Unsupported readonly request")
                    .kv("requestType", message.type())
                    .toString();
            return CompletableFuture.failedFuture(new UnsupportedOperationException(exMsg));
        }
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RequestMessage message;
        if (trx.getClientRequest() != null
                && trx.getClientRequest().getMessage() instanceof RequestMessage localMessage) {
            message = localMessage;
        } else {
            try {
                message = RequestMessage.fromBytes(trx.getStateMachineLogEntry().getLogData());
            } catch (InvalidProtocolBufferException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        if (message.message() instanceof RegisterBookieRequest req) {
            final RegisterBookieReply.Builder reply = RegisterBookieReply.newBuilder();
            reply.setExist(!state.registerBookie(req.getService()));
            final ByteString content =
                    BufferUtils.byteStringDoShade(reply.build().toByteString());
            return CompletableFuture.completedFuture(Message.valueOf(content));
        } else {
            final String exMsg = ExceptionMessageBuilder.exMsg("Unsupported readonly request")
                    .kv("requestType", message.type())
                    .toString();
            return CompletableFuture.failedFuture(new UnsupportedOperationException(exMsg));
        }
    }
}
