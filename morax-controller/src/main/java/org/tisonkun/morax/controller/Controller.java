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
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;
import org.tisonkun.morax.proto.config.ControllerServerConfig;
import org.tisonkun.morax.proto.controller.ControllerGrpc;
import org.tisonkun.morax.proto.controller.ControllerRequestType;
import org.tisonkun.morax.proto.controller.ListBookiesReply;
import org.tisonkun.morax.proto.controller.ListBookiesRequest;
import org.tisonkun.morax.proto.controller.RegisterBookieReply;
import org.tisonkun.morax.proto.controller.RegisterBookieRequest;

@Slf4j
public class Controller extends AbstractIdleService {
    // currently - the controller quorum is always in the same group
    private static final RaftGroupId RAFT_GROUP_ID =
            RaftGroupId.valueOf(UUID.nameUUIDFromBytes("MORAX".getBytes(StandardCharsets.UTF_8)));

    private final RaftServer raftServer;
    private final RaftClient raftClient;
    private final Server grpcServer;

    public Controller(ControllerServerConfig config) throws IOException {
        final Collection<RaftPeer> peers = config.getRaftGroup().getPeers().stream()
                .map(p -> RaftPeer.newBuilder()
                        .setId(p.getId())
                        .setAddress(p.getAddress())
                        .build())
                .collect(Collectors.toSet());

        final RaftPeer peer = peers.stream()
                .filter(p -> p.getId().toString().equals(config.getRaftPeerId()))
                .findFirst()
                .orElseThrow();

        final RaftGroup group = RaftGroup.valueOf(RAFT_GROUP_ID, peer);

        final RaftProperties properties = new RaftProperties();
        final int raftPeerPort = NetUtils.createSocketAddr(peer.getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, raftPeerPort);
        RaftServerConfigKeys.setStorageDir(properties, config.getRaftStorageDir());

        this.raftServer = RaftServer.newBuilder()
                .setGroup(group)
                .setProperties(properties)
                .setServerId(peer.getId())
                .setStateMachine(new ControllerStateMachine())
                .build();

        final GrpcClientRpc rpc = new GrpcFactory(new Parameters()).newRaftClientRpc(ClientId.randomId(), properties);
        this.raftClient = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setClientRpc(rpc)
                .build();

        this.grpcServer = ServerBuilder.forPort(config.getServerPort())
                .addService(new GrpcServiceAdapter())
                .build();
    }

    @Override
    protected void startUp() throws Exception {
        this.raftServer.start();
        final int port = this.grpcServer.start().getPort();
        log.info("Controller has been ready at port {}.", port);
    }

    @Override
    protected void shutDown() throws Exception {
        this.grpcServer.shutdown().awaitTermination();
        this.raftServer.close();
        log.info("Controller has been shutdown.");
    }

    public RegisterBookieReply registerService(RegisterBookieRequest request) throws IOException {
        final RequestMessage message = new RequestMessage(ControllerRequestType.RegisterBookie, request);
        final RaftClientReply reply = this.raftClient.io().send(message);
        return RegisterBookieReply.parseFrom(reply.getMessage().getContent().asReadOnlyByteBuffer());
    }

    public ListBookiesReply listServices(ListBookiesRequest request) throws IOException {
        final RequestMessage message = new RequestMessage(ControllerRequestType.ListBookies, request);
        final RaftClientReply reply = this.raftClient.io().sendReadOnly(message);
        return ListBookiesReply.parseFrom(reply.getMessage().getContent().asReadOnlyByteBuffer());
    }

    private class GrpcServiceAdapter extends ControllerGrpc.ControllerImplBase {
        @Override
        public void listBookies(ListBookiesRequest request, StreamObserver<ListBookiesReply> responseObserver) {
            try {
                final ListBookiesReply reply = Controller.this.listServices(request);
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }

        @Override
        public void registerBookie(
                RegisterBookieRequest request, StreamObserver<RegisterBookieReply> responseObserver) {
            try {
                final RegisterBookieReply reply = Controller.this.registerService(request);
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }
    }
}
