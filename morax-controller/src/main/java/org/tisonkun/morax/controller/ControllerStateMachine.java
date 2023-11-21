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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.tisonkun.morax.proto.controller.ListServicesReply;
import org.tisonkun.morax.proto.controller.RegisterServiceReply;
import org.tisonkun.morax.proto.controller.RegisterServiceRequest;
import org.tisonkun.morax.proto.controller.RequestUnion;
import org.tisonkun.morax.proto.controller.ServiceType;
import org.tisonkun.morax.proto.exception.ExceptionMessageBuilder;
import org.tisonkun.morax.proto.io.BufferUtils;

public class ControllerStateMachine extends BaseStateMachine {

    private final ControllerState state = new ControllerState();

    @Override
    public CompletableFuture<Message> query(Message request) {
        final RequestUnion requestUnion;

        if (request instanceof ProtoMessage localMessage) {
            requestUnion = (RequestUnion) localMessage.message();
        } else {
            try {
                requestUnion = RequestUnion.parseFrom(BufferUtils.byteStringUndoShade(request.getContent()));
            } catch (InvalidProtocolBufferException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        //noinspection SwitchStatementWithTooFewBranches
        switch (requestUnion.getRequestUnionCase()) {
            case LISTSERVICES -> {
                final List<ServiceType> serviceTypes =
                        requestUnion.getListServices().getServiceTypeList();
                final ListServicesReply.Builder reply = ListServicesReply.newBuilder();
                reply.addAllServiceInfo(state.listServices(serviceTypes));
                return CompletableFuture.completedFuture(new ProtoMessage(reply.build()));
            }
            default -> {
                final String message = ExceptionMessageBuilder.exMsg("Unsupported readonly request")
                        .kv("requestCase", requestUnion.getRequestUnionCase())
                        .toString();
                return CompletableFuture.failedFuture(new UnsupportedOperationException(message));
            }
        }
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RequestUnion requestUnion;

        if (trx.getClientRequest() != null
                && trx.getClientRequest().getMessage() instanceof ProtoMessage localMessage) {
            requestUnion = (RequestUnion) localMessage.message();
        } else {
            try {
                requestUnion = RequestUnion.parseFrom(BufferUtils.byteStringUndoShade(
                        trx.getStateMachineLogEntry().getLogData()));
            } catch (InvalidProtocolBufferException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        //noinspection SwitchStatementWithTooFewBranches
        switch (requestUnion.getRequestUnionCase()) {
            case REGISTERSERVICE -> {
                final RegisterServiceRequest registerServiceRequest = requestUnion.getRegisterService();
                final RegisterServiceReply.Builder reply = RegisterServiceReply.newBuilder();
                reply.setExist(!state.registerService(registerServiceRequest.getServiceInfo()));
                return CompletableFuture.completedFuture(new ProtoMessage(reply.build()));
            }
            default -> {
                final String message = ExceptionMessageBuilder.exMsg("Unsupported write request")
                        .kv("requestCase", requestUnion.getRequestUnionCase())
                        .toString();
                return CompletableFuture.failedFuture(new UnsupportedOperationException(message));
            }
        }
    }
}
