/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tisonkun.morax.rpc.network.client;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tisonkun.morax.rpc.network.io.TransportFrameDecoder;
import org.tisonkun.morax.rpc.network.protocol.ChunkFetchFailure;
import org.tisonkun.morax.rpc.network.protocol.ChunkFetchSuccess;
import org.tisonkun.morax.rpc.network.protocol.MergedBlockMetaSuccess;
import org.tisonkun.morax.rpc.network.protocol.ResponseMessage;
import org.tisonkun.morax.rpc.network.protocol.RpcFailure;
import org.tisonkun.morax.rpc.network.protocol.RpcResponse;
import org.tisonkun.morax.rpc.network.protocol.StreamChunkId;
import org.tisonkun.morax.rpc.network.protocol.StreamFailure;
import org.tisonkun.morax.rpc.network.protocol.StreamResponse;
import org.tisonkun.morax.rpc.network.server.MessageHandler;
import org.tisonkun.morax.rpc.network.util.NettyUtils;

/**
 * Handler that processes server responses, in response to requests issued from a
 * {@link TransportClient}. It works by tracking the list of outstanding requests (and their callbacks).
 * <p>
 * Concurrency: thread safe and can be called from multiple threads.
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

    private final Channel channel;

    private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

    private final Map<Long, BaseResponseCallback> outstandingRPCs;

    private final Queue<Pair<String, StreamCallback>> streamCallbacks;
    private volatile boolean streamActive;

    /**
     * Records the time (in system nanoseconds) that the last fetch or RPC request was sent.
     */
    private final AtomicLong timeOfLastRequestNs;

    public TransportResponseHandler(Channel channel) {
        this.channel = channel;
        this.outstandingFetches = new ConcurrentHashMap<>();
        this.outstandingRPCs = new ConcurrentHashMap<>();
        this.streamCallbacks = new ConcurrentLinkedQueue<>();
        this.timeOfLastRequestNs = new AtomicLong(0);
    }

    public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
        updateTimeOfLastRequest();
        outstandingFetches.put(streamChunkId, callback);
    }

    public void removeFetchRequest(StreamChunkId streamChunkId) {
        outstandingFetches.remove(streamChunkId);
    }

    public void addRpcRequest(long requestId, BaseResponseCallback callback) {
        updateTimeOfLastRequest();
        outstandingRPCs.put(requestId, callback);
    }

    public void removeRpcRequest(long requestId) {
        outstandingRPCs.remove(requestId);
    }

    public void addStreamCallback(String streamId, StreamCallback callback) {
        updateTimeOfLastRequest();
        streamCallbacks.offer(ImmutablePair.of(streamId, callback));
    }

    @VisibleForTesting
    public void deactivateStream() {
        streamActive = false;
    }

    /**
     * Fire the failure callback for all outstanding requests. This is called when we have an
     * uncaught exception or pre-mature connection termination.
     */
    private void failOutstandingRequests(Throwable cause) {
        for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
            try {
                entry.getValue().onFailure(entry.getKey().chunkIndex, cause);
            } catch (Exception e) {
                logger.warn("ChunkReceivedCallback.onFailure throws exception", e);
            }
        }
        for (BaseResponseCallback callback : outstandingRPCs.values()) {
            try {
                callback.onFailure(cause);
            } catch (Exception e) {
                logger.warn("RpcResponseCallback.onFailure throws exception", e);
            }
        }
        for (Pair<String, StreamCallback> entry : streamCallbacks) {
            try {
                entry.getValue().onFailure(entry.getKey(), cause);
            } catch (Exception e) {
                logger.warn("StreamCallback.onFailure throws exception", e);
            }
        }

        // It's OK if new fetches appear, as they will fail immediately.
        outstandingFetches.clear();
        outstandingRPCs.clear();
        streamCallbacks.clear();
    }

    @Override
    public void channelActive() {}

    @Override
    public void channelInactive() {
        if (hasOutstandingRequests()) {
            String remoteAddress = NettyUtils.getRemoteAddress(channel);
            logger.error(
                    "Still have {} requests outstanding when connection from {} is closed",
                    numOutstandingRequests(),
                    remoteAddress);
            failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
        }
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        if (hasOutstandingRequests()) {
            String remoteAddress = NettyUtils.getRemoteAddress(channel);
            logger.error(
                    "Still have {} requests outstanding when connection from {} is closed",
                    numOutstandingRequests(),
                    remoteAddress);
            failOutstandingRequests(cause);
        }
    }

    @Override
    public void handle(ResponseMessage message) throws Exception {
        if (message instanceof ChunkFetchSuccess) {
            ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                logger.warn(
                        "Ignoring response for block {} from {} since it is not outstanding",
                        resp.streamChunkId,
                        NettyUtils.getRemoteAddress(channel));
                resp.body().release();
            } else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
                resp.body().release();
            }
        } else if (message instanceof ChunkFetchFailure) {
            ChunkFetchFailure resp = (ChunkFetchFailure) message;
            ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
            if (listener == null) {
                logger.warn(
                        "Ignoring response for block {} from {} ({}) since it is not outstanding",
                        resp.streamChunkId,
                        NettyUtils.getRemoteAddress(channel),
                        resp.errorString);
            } else {
                outstandingFetches.remove(resp.streamChunkId);
                listener.onFailure(
                        resp.streamChunkId.chunkIndex,
                        new ChunkFetchFailureException(
                                "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
            }
        } else if (message instanceof RpcResponse) {
            RpcResponse resp = (RpcResponse) message;
            RpcResponseCallback listener = (RpcResponseCallback) outstandingRPCs.get(resp.requestId);
            if (listener == null) {
                logger.warn(
                        "Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                        resp.requestId,
                        NettyUtils.getRemoteAddress(channel),
                        resp.body().size());
                resp.body().release();
            } else {
                outstandingRPCs.remove(resp.requestId);
                try {
                    listener.onSuccess(resp.body().nioByteBuffer());
                } finally {
                    resp.body().release();
                }
            }
        } else if (message instanceof RpcFailure) {
            RpcFailure resp = (RpcFailure) message;
            BaseResponseCallback listener = outstandingRPCs.get(resp.requestId);
            if (listener == null) {
                logger.warn(
                        "Ignoring response for RPC {} from {} ({}) since it is not outstanding",
                        resp.requestId,
                        NettyUtils.getRemoteAddress(channel),
                        resp.errorString);
            } else {
                outstandingRPCs.remove(resp.requestId);
                listener.onFailure(new RuntimeException(resp.errorString));
            }
        } else if (message instanceof MergedBlockMetaSuccess) {
            MergedBlockMetaSuccess resp = (MergedBlockMetaSuccess) message;
            try {
                MergedBlockMetaResponseCallback listener =
                        (MergedBlockMetaResponseCallback) outstandingRPCs.get(resp.requestId);
                if (listener == null) {
                    logger.warn(
                            "Ignoring response for MergedBlockMetaRequest {} from {} ({} bytes) since it is not"
                                    + " outstanding",
                            resp.requestId,
                            NettyUtils.getRemoteAddress(channel),
                            resp.body().size());
                } else {
                    outstandingRPCs.remove(resp.requestId);
                    listener.onSuccess(resp.numChunks, resp.body());
                }
            } finally {
                resp.body().release();
            }
        } else if (message instanceof StreamResponse) {
            StreamResponse resp = (StreamResponse) message;
            Pair<String, StreamCallback> entry = streamCallbacks.poll();
            if (entry != null) {
                StreamCallback callback = entry.getValue();
                if (resp.byteCount > 0) {
                    StreamInterceptor<ResponseMessage> interceptor =
                            new StreamInterceptor<>(this, resp.streamId, resp.byteCount, callback);
                    try {
                        TransportFrameDecoder frameDecoder =
                                (TransportFrameDecoder) channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
                        frameDecoder.setInterceptor(interceptor);
                        streamActive = true;
                    } catch (Exception e) {
                        logger.error("Error installing stream handler.", e);
                        deactivateStream();
                    }
                } else {
                    try {
                        callback.onComplete(resp.streamId);
                    } catch (Exception e) {
                        logger.warn("Error in stream handler onComplete().", e);
                    }
                }
            } else {
                logger.error("Could not find callback for StreamResponse.");
            }
        } else if (message instanceof StreamFailure) {
            StreamFailure resp = (StreamFailure) message;
            Pair<String, StreamCallback> entry = streamCallbacks.poll();
            if (entry != null) {
                StreamCallback callback = entry.getValue();
                try {
                    callback.onFailure(resp.streamId, new RuntimeException(resp.error));
                } catch (IOException ioe) {
                    logger.warn("Error in stream failure handler.", ioe);
                }
            } else {
                logger.warn("Stream failure with unknown callback: {}", resp.error);
            }
        } else {
            throw new IllegalStateException("Unknown response type: " + message.type());
        }
    }

    /**
     * Returns total number of outstanding requests (fetch requests + RPCs)
     */
    public int numOutstandingRequests() {
        return outstandingFetches.size() + outstandingRPCs.size() + streamCallbacks.size() + (streamActive ? 1 : 0);
    }

    /**
     * Check if there are any outstanding requests (fetch requests + RPCs)
     */
    public Boolean hasOutstandingRequests() {
        return streamActive
                || !outstandingFetches.isEmpty()
                || !outstandingRPCs.isEmpty()
                || !streamCallbacks.isEmpty();
    }

    /**
     * Returns the time in nanoseconds of when the last request was sent out.
     */
    public long getTimeOfLastRequestNs() {
        return timeOfLastRequestNs.get();
    }

    /**
     * Updates the time of the last request to the current system time.
     */
    public void updateTimeOfLastRequest() {
        timeOfLastRequestNs.set(System.nanoTime());
    }
}
