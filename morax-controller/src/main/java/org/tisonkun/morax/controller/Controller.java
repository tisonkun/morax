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

import com.google.common.util.concurrent.AbstractIdleService;
import java.io.IOException;
import java.util.UUID;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.client.GrpcClientRpc;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.NetUtils;
import org.tisonkun.morax.proto.config.MoraxControllerServerConfig;
import org.tisonkun.morax.proto.controller.ListServicesReply;
import org.tisonkun.morax.proto.controller.ListServicesRequest;
import org.tisonkun.morax.proto.controller.RegisterServiceReply;
import org.tisonkun.morax.proto.controller.RegisterServiceRequest;
import org.tisonkun.morax.proto.controller.ServiceInfoProto;
import org.tisonkun.morax.proto.controller.ServiceType;

public class Controller extends AbstractIdleService {
    private final RaftGroupId raftGroupId;
    private final RaftServer raftServer;
    private final RaftClient raftClient;

    public Controller(MoraxControllerServerConfig config) throws IOException {
        final String address = "127.0.0.1:" + config.getRaftServerPort();
        final RaftPeer peer =
                RaftPeer.newBuilder().setId("n0").setAddress(address).build();
        final int port = NetUtils.createSocketAddr(address).getPort();
        final RaftProperties properties = new RaftProperties();
        GrpcConfigKeys.Server.setPort(properties, port);

        this.raftGroupId = RaftGroupId.valueOf(new UUID(0, 1));
        final RaftGroup group = RaftGroup.valueOf(this.raftGroupId, peer);
        this.raftServer = RaftServer.newBuilder()
                .setGroup(group)
                .setProperties(properties)
                .setServerId(peer.getId())
                .setStateMachine(new ControllerStateMachine())
                .build();

        final GrpcClientRpc rpc =
                new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties);
        this.raftClient = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setClientRpc(rpc)
                .build();
    }

    @Override
    protected void startUp() throws Exception {
        this.raftServer.start();
    }

    @Override
    protected void shutDown() throws Exception {
        this.raftServer.close();
    }

    public RegisterServiceReply registerService(RegisterServiceRequest request) throws IOException {
        final RaftClientReply reply = this.raftClient.io().send(new LocalMessage(request));
        return RegisterServiceReply.parseFrom(reply.getMessage().getContent().asReadOnlyByteBuffer());
    }

    public ListServicesReply listServices(ListServicesRequest request) throws IOException {
        final RaftClientReply reply = this.raftClient.io().sendReadOnly(new LocalMessage(request));
        return ListServicesReply.parseFrom(reply.getMessage().getContent().asReadOnlyByteBuffer());
    }

    public static void main(String[] args) throws Exception {
        final Controller stateManager =
                new Controller(MoraxControllerServerConfig.builder().build());
        try {
            stateManager.startUp();
            {
                final ListServicesReply listServicesReply = stateManager.listServices(ListServicesRequest.newBuilder()
                        .addServiceType(ServiceType.Bookie)
                        .build());
                System.out.println("listServicesReply=" + listServicesReply);
            }
            {
                final ServiceInfoProto serviceInfoProto = ServiceInfoProto.newBuilder()
                        .setType(ServiceType.Bookie)
                        .setTarget("localhost:8080")
                        .build();
                final RegisterServiceReply registerServiceReply =
                        stateManager.registerService(RegisterServiceRequest.newBuilder()
                                .setServiceInfo(serviceInfoProto)
                                .build());
                System.out.println("registerServiceReply=" + registerServiceReply);
            }
            {
                final ListServicesReply listServicesReply = stateManager.listServices(ListServicesRequest.newBuilder()
                        .addServiceType(ServiceType.Bookie)
                        .build());
                System.out.println("listServicesReply=" + listServicesReply);
            }
        } finally {
            stateManager.shutDown();
        }
    }
}
