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

package org.tisonkun.morax.bookie;

import io.grpc.stub.StreamObserver;
import org.tisonkun.morax.proto.bookie.AddEntryReply;
import org.tisonkun.morax.proto.bookie.AddEntryRequest;
import org.tisonkun.morax.proto.bookie.BookieServiceGrpc;
import org.tisonkun.morax.proto.bookie.Entry;
import org.tisonkun.morax.proto.bookie.ReadEntryReply;
import org.tisonkun.morax.proto.bookie.ReadEntryRequest;
import org.tisonkun.morax.proto.config.MoraxBookieServerConfig;

public class BookieService extends BookieServiceGrpc.BookieServiceImplBase {
    private final Bookie bookie;

    public BookieService(MoraxBookieServerConfig serverConfig) {
        this.bookie = new Bookie(serverConfig);
    }

    @Override
    public void addEntry(AddEntryRequest request, StreamObserver<AddEntryReply> responseObserver) {
        final Entry entry = Entry.fromProtos(request.getEntry());
        try {
            bookie.addEntry(entry);
            responseObserver.onNext(AddEntryReply.newBuilder()
                    .setLedgerId(entry.getLedgerId())
                    .setEntryId(entry.getEntryId())
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void readEntry(ReadEntryRequest request, StreamObserver<ReadEntryReply> responseObserver) {
        final long ledgerId = request.getLedgerId();
        final long entryId = request.getEntryId();
        try {
            final Entry entry = bookie.readEntry(ledgerId, entryId);
            responseObserver.onNext(
                    ReadEntryReply.newBuilder().setEntry(entry.toEntryProto()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
