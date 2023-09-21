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

import io.grpc.stub.StreamObserver;
import org.tisonkun.morax.proto.controller.ControllerGrpc;
import org.tisonkun.morax.proto.controller.ListServicesReply;
import org.tisonkun.morax.proto.controller.ListServicesRequest;
import org.tisonkun.morax.proto.controller.RegisterServiceReply;
import org.tisonkun.morax.proto.controller.RegisterServiceRequest;

public class ControllerService extends ControllerGrpc.ControllerImplBase {
    private final Controller controller;

    public ControllerService(Controller controller) {
        this.controller = controller;
    }

    @Override
    public void listServices(ListServicesRequest request, StreamObserver<ListServicesReply> responseObserver) {
        try {
            final ListServicesReply reply = controller.listServices(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void registerService(RegisterServiceRequest request, StreamObserver<RegisterServiceReply> responseObserver) {
        try {
            final RegisterServiceReply reply = controller.registerService(request);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
