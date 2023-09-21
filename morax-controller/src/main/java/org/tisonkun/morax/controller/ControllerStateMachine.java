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

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.tisonkun.morax.proto.controller.ListServicesReply;
import org.tisonkun.morax.proto.controller.ListServicesRequest;
import org.tisonkun.morax.proto.controller.RegisterServiceReply;
import org.tisonkun.morax.proto.controller.RegisterServiceRequest;
import org.tisonkun.morax.proto.controller.ServiceInfoProto;
import org.tisonkun.morax.proto.controller.ServiceType;
import org.tisonkun.morax.proto.io.BufferUtils;

public class ControllerStateMachine extends BaseStateMachine {
    private final Map<ServiceType, Collection<ServiceInfoProto>> services = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Message> query(Message request) {
        final ListServicesRequest listServicesRequest;
        if (request instanceof LocalMessage localMessage) {
            final GeneratedMessageV3 generatedMessage = localMessage.getActualMessage();
            listServicesRequest = (ListServicesRequest) generatedMessage;
        } else {
            try {
                final ByteString bytes = BufferUtils.byteStringUndoShade(request.getContent());
                listServicesRequest = ListServicesRequest.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        final List<ServiceType> serviceTypes = listServicesRequest.getServiceTypeList();
        final ListServicesReply.Builder reply = ListServicesReply.newBuilder();
        for (ServiceType serviceType : serviceTypes) {
            final Collection<ServiceInfoProto> serviceInfos =
                    services.getOrDefault(serviceType, Collections.emptySet());
            reply.addAllServiceInfo(serviceInfos);
        }
        return CompletableFuture.completedFuture(new LocalMessage(reply.build()));
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RegisterServiceRequest registerServiceRequest;
        if (trx.getClientRequest() != null
                && trx.getClientRequest().getMessage() instanceof LocalMessage localMessage) {
            final GeneratedMessageV3 generatedMessage = localMessage.getActualMessage();
            registerServiceRequest = (RegisterServiceRequest) generatedMessage;
        } else {
            try {
                final ByteString bytes = BufferUtils.byteStringUndoShade(
                        trx.getStateMachineLogEntry().getLogData());
                registerServiceRequest = RegisterServiceRequest.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        final ServiceInfoProto serviceInfo = registerServiceRequest.getServiceInfo();
        final ServiceType serviceType = serviceInfo.getType();
        final Collection<ServiceInfoProto> serviceInfoProtos = services.computeIfAbsent(
                serviceType,
                ignore -> new ConcurrentSkipListSet<>(
                        Comparator.comparing(ServiceInfoProto::getTarget).thenComparing(ServiceInfoProto::getType)));
        serviceInfoProtos.add(serviceInfo);
        final RegisterServiceReply.Builder reply = RegisterServiceReply.newBuilder();
        return CompletableFuture.completedFuture(new LocalMessage(reply.build()));
    }
}
