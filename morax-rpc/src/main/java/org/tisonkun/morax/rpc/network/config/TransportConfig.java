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

package org.tisonkun.morax.rpc.network.config;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.netty.util.NettyRuntime;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.tisonkun.morax.rpc.network.server.TransportServer;
import org.tisonkun.morax.rpc.network.util.CryptoUtils;
import org.tisonkun.morax.rpc.network.util.JavaUtils;

/**
 * A central location that tracks all the settings we expose to users.
 */
public class TransportConfig {

    private final String networkIoModeConfigKey;
    private final String networkIoPreferDirectBufsConfigKey;
    private final String networkIoConnectionTimeoutConfigKey;
    private final String networkIoConnectionCreationTimeoutConfigKey;
    private final String networkIoBacklogConfigKey;
    private final String networkIoNumConnectionsPerPeerConfigKey;
    private final String networkIoServerThreadsConfigKey;
    private final String networkIoClientThreadsConfigKey;
    private final String networkIoReceiveBufferConfigKey;
    private final String networkIoSendBufferConfigKey;
    private final String networkSaslTimeoutConfigKey;
    private final String networkIoMaxRetriesConfigKey;
    private final String networkIoRetryWaitConfigKey;
    private final String networkIoLazyFDConfigKey;
    private final String networkVerboseMetricsConfigKey;
    private final String networkIoEnableTCPKeepAliveConfigKey;

    private final ConfigProvider conf;

    private final String module;

    public TransportConfig(String module, ConfigProvider conf) {
        this.module = module;
        this.conf = conf;
        networkIoModeConfigKey = getConfKey("io.mode");
        networkIoPreferDirectBufsConfigKey = getConfKey("io.preferDirectBufs");
        networkIoConnectionTimeoutConfigKey = getConfKey("io.connectionTimeout");
        networkIoConnectionCreationTimeoutConfigKey = getConfKey("io.connectionCreationTimeout");
        networkIoBacklogConfigKey = getConfKey("io.backLog");
        networkIoNumConnectionsPerPeerConfigKey = getConfKey("io.numConnectionsPerPeer");
        networkIoServerThreadsConfigKey = getConfKey("io.serverThreads");
        networkIoClientThreadsConfigKey = getConfKey("io.clientThreads");
        networkIoReceiveBufferConfigKey = getConfKey("io.receiveBuffer");
        networkIoSendBufferConfigKey = getConfKey("io.sendBuffer");
        networkSaslTimeoutConfigKey = getConfKey("sasl.timeout");
        networkIoMaxRetriesConfigKey = getConfKey("io.maxRetries");
        networkIoRetryWaitConfigKey = getConfKey("io.retryWait");
        networkIoLazyFDConfigKey = getConfKey("io.lazyFD");
        networkVerboseMetricsConfigKey = getConfKey("io.enableVerboseMetrics");
        networkIoEnableTCPKeepAliveConfigKey = getConfKey("io.enableTcpKeepAlive");
    }

    public int getInt(String name, int defaultValue) {
        return conf.getInt(name, defaultValue);
    }

    public String get(String name, String defaultValue) {
        return conf.get(name, defaultValue);
    }

    private String getConfKey(String suffix) {
        return "morax." + module + "." + suffix;
    }

    public String getModuleName() {
        return module;
    }

    /** IO mode: nio or epoll */
    public String ioMode() {
        return conf.get(networkIoModeConfigKey, "NIO").toUpperCase(Locale.ROOT);
    }

    /** If true, we will prefer allocating off-heap byte buffers within Netty. */
    public boolean preferDirectBufs() {
        return conf.getBoolean(networkIoPreferDirectBufsConfigKey, true);
    }

    /** Connection idle timeout in milliseconds. Default 120 secs. */
    public int connectionTimeoutMs() {
        long defaultNetworkTimeoutS = JavaUtils.timeStringAsSec(conf.get("morax.network.timeout", "120s"));
        long defaultTimeoutMs =
                JavaUtils.timeStringAsSec(conf.get(networkIoConnectionTimeoutConfigKey, defaultNetworkTimeoutS + "s"))
                        * 1000;
        return defaultTimeoutMs < 0 ? 0 : (int) defaultTimeoutMs;
    }

    /** Connect creation timeout in milliseconds. Default 120 secs. */
    public int connectionCreationTimeoutMs() {
        long connectionTimeoutS = TimeUnit.MILLISECONDS.toSeconds(connectionTimeoutMs());
        long defaultTimeoutMs = JavaUtils.timeStringAsSec(
                        conf.get(networkIoConnectionCreationTimeoutConfigKey, connectionTimeoutS + "s"))
                * 1000;
        return defaultTimeoutMs < 0 ? 0 : (int) defaultTimeoutMs;
    }

    /** Number of concurrent connections between two nodes for fetching data. */
    public int numConnectionsPerPeer() {
        return conf.getInt(networkIoNumConnectionsPerPeerConfigKey, 1);
    }

    /**
     * Requested maximum length of the queue of incoming connections. If  &lt; 1,
     * the default Netty value of {@link io.netty.util.NetUtil#SOMAXCONN} will be used.
     * Default to -1.
     */
    public int backLog() {
        return conf.getInt(networkIoBacklogConfigKey, -1);
    }

    /** Number of threads used in the server thread pool. Default to 0, which is 2x#cores. */
    public int serverThreads() {
        return conf.getInt(networkIoServerThreadsConfigKey, 0);
    }

    /** Number of threads used in the client thread pool. Default to 0, which is 2x#cores. */
    public int clientThreads() {
        return conf.getInt(networkIoClientThreadsConfigKey, 0);
    }

    /**
     * Receive buffer size (SO_RCVBUF).
     * Note: the optimal size for receive buffer and send buffer should be
     *  latency * network_bandwidth.
     * Assuming latency = 1ms, network_bandwidth = 10Gbps
     *  buffer size should be ~ 1.25MB
     */
    public int receiveBuf() {
        return conf.getInt(networkIoReceiveBufferConfigKey, -1);
    }

    /** Send buffer size (SO_SNDBUF). */
    public int sendBuf() {
        return conf.getInt(networkIoSendBufferConfigKey, -1);
    }

    /** Timeout for a single round trip of auth message exchange, in milliseconds. */
    public int authRTTimeoutMs() {
        return (int) JavaUtils.timeStringAsSec(
                        conf.get("morax.network.auth.rpcTimeout", conf.get(networkSaslTimeoutConfigKey, "30s")))
                * 1000;
    }

    /**
     * Max number of times we will try IO exceptions (such as connection timeouts) per request.
     * If set to 0, we will not do any retries.
     */
    public int maxIORetries() {
        return conf.getInt(networkIoMaxRetriesConfigKey, 3);
    }

    /**
     * Time (in milliseconds) that we will wait in order to perform a retry after an IOException.
     * Only relevant if maxIORetries &gt; 0.
     */
    public int ioRetryWaitTimeMs() {
        return (int) JavaUtils.timeStringAsSec(conf.get(networkIoRetryWaitConfigKey, "5s")) * 1000;
    }

    /**
     * Minimum size of a block that we should start using memory map rather than reading in through
     * normal IO operations. This prevents morax from memory mapping very small blocks. In general,
     * memory mapping has high overhead for blocks close to or below the page size of the OS.
     */
    public int memoryMapBytes() {
        return Ints.checkedCast(JavaUtils.byteStringAsBytes(conf.get("morax.storage.memoryMapThreshold", "2m")));
    }

    /**
     * Whether to initialize FileDescriptor lazily or not. If true, file descriptors are
     * created only when data is going to be transferred. This can reduce the number of open files.
     */
    public boolean lazyFileDescriptor() {
        return conf.getBoolean(networkIoLazyFDConfigKey, true);
    }

    /**
     * Whether to track Netty memory detailed metrics. If true, the detailed metrics of Netty
     * PoolByteBufAllocator will be gotten, otherwise only general memory usage will be tracked.
     */
    public boolean verboseMetrics() {
        return conf.getBoolean(networkVerboseMetricsConfigKey, false);
    }

    /**
     * Whether to enable TCP keep-alive. If true, the TCP keep-alives are enabled, which removes
     * connections that are idle for too long.
     */
    public boolean enableTcpKeepAlive() {
        return conf.getBoolean(networkIoEnableTCPKeepAliveConfigKey, false);
    }

    /**
     * Maximum number of retries when binding to a port before giving up.
     */
    public int portMaxRetries() {
        return conf.getInt("morax.port.maxRetries", 16);
    }

    /**
     * Enables strong encryption. Also enables the new auth protocol, used to negotiate keys.
     */
    public boolean encryptionEnabled() {
        return conf.getBoolean("morax.network.crypto.enabled", false);
    }

    /**
     * The cipher transformation to use for encrypting session data.
     */
    public String cipherTransformation() {
        return conf.get("morax.network.crypto.cipher", "AES/CTR/NoPadding");
    }

    /**
     * Whether to fall back to SASL if the new auth protocol fails. Enabled by default for
     * backwards compatibility.
     */
    public boolean saslFallback() {
        return conf.getBoolean("morax.network.crypto.saslFallback", true);
    }

    /**
     * Whether to enable SASL-based encryption when authenticating using SASL.
     */
    public boolean saslEncryption() {
        return conf.getBoolean("morax.authenticate.enableSaslEncryption", false);
    }

    /**
     * Maximum number of bytes to be encrypted at a time when SASL encryption is used.
     */
    public int maxSaslEncryptedBlockSize() {
        return Ints.checkedCast(
                JavaUtils.byteStringAsBytes(conf.get("morax.network.sasl.maxEncryptedBlockSize", "64k")));
    }

    /**
     * Whether the server should enforce encryption on SASL-authenticated connections.
     */
    public boolean saslServerAlwaysEncrypt() {
        return conf.getBoolean("morax.network.sasl.serverAlwaysEncrypt", false);
    }

    /**
     * Flag indicating whether to share the pooled ByteBuf allocators between the different Netty
     * channels. If enabled then only two pooled ByteBuf allocators are created: one where caching
     * is allowed (for transport servers) and one where not (for transport clients).
     * When disabled a new allocator is created for each transport servers and clients.
     */
    public boolean sharedByteBufAllocators() {
        return conf.getBoolean("morax.network.sharedByteBufAllocators.enabled", true);
    }

    /**
     * If enabled then off-heap byte buffers will be preferred for the shared ByteBuf allocators.
     */
    public boolean preferDirectBufsForSharedByteBufAllocators() {
        return conf.getBoolean("morax.network.io.preferDirectBufs", true);
    }

    /**
     * The commons-crypto configuration for the module.
     */
    public Properties cryptoConf() {
        return CryptoUtils.toCryptoConf("morax.network.crypto.config.", conf.getAll());
    }

    /**
     * The max number of chunks allowed to be transferred at the same time on shuffle service.
     * Note that new incoming connections will be closed when the max number is hit. The client will
     * retry according to the shuffle retry configs (see `morax.shuffle.io.maxRetries` and
     * `morax.shuffle.io.retryWait`), if those limits are reached the task will fail with fetch
     * failure.
     */
    public long maxChunksBeingTransferred() {
        return conf.getLong("morax.shuffle.maxChunksBeingTransferred", Long.MAX_VALUE);
    }

    /**
     * Percentage of io.serverThreads used by netty to process ChunkFetchRequest.
     * When the config `morax.shuffle.server.chunkFetchHandlerThreadsPercent` is set,
     * shuffle server will use a separate EventLoopGroup to process ChunkFetchRequest messages.
     * Although when calling the async writeAndFlush on the underlying channel to send
     * response back to client, the I/O on the channel is still being handled by
     * {@link TransportServer}'s default EventLoopGroup
     * that's registered with the Channel, by waiting inside the ChunkFetchRequest handler
     * threads for the completion of sending back responses, we are able to put a limit on
     * the max number of threads from TransportServer's default EventLoopGroup that are
     * going to be consumed by writing response to ChunkFetchRequest, which are I/O intensive
     * and could take long time to process due to disk contentions. By configuring a slightly
     * higher number of shuffler server threads, we are able to reserve some threads for
     * handling other RPC messages, thus making the Client less likely to experience timeout
     * when sending RPC messages to the shuffle server. The number of threads used for handling
     * chunked fetch requests are percentage of io.serverThreads (if defined) else it is a percentage
     * of 2 * #cores. However, a percentage of 0 means netty default number of threads which
     * is 2 * #cores ignoring io.serverThreads. The percentage here is configured via
     * morax.shuffle.server.chunkFetchHandlerThreadsPercent. The returned value is rounded off to
     * ceiling of the nearest integer.
     */
    public int chunkFetchHandlerThreads() {
        if (!this.getModuleName().equalsIgnoreCase("shuffle")) {
            return 0;
        }
        int chunkFetchHandlerThreadsPercent =
                Integer.parseInt(conf.get("morax.shuffle.server.chunkFetchHandlerThreadsPercent"));
        int threads = this.serverThreads() > 0 ? this.serverThreads() : 2 * NettyRuntime.availableProcessors();
        return (int) Math.ceil(threads * (chunkFetchHandlerThreadsPercent / 100.0));
    }

    /**
     * Whether to use a separate EventLoopGroup to process ChunkFetchRequest messages, it is decided
     * by the config `morax.shuffle.server.chunkFetchHandlerThreadsPercent` is set or not.
     */
    public boolean separateChunkFetchRequest() {
        return conf.getInt("morax.shuffle.server.chunkFetchHandlerThreadsPercent", 0) > 0;
    }

    /**
     * Percentage of io.serverThreads used by netty to process FinalizeShuffleMerge. When the config
     * `morax.shuffle.server.finalizeShuffleMergeThreadsPercent` is set, shuffle server will use a
     * separate EventLoopGroup to process FinalizeShuffleMerge messages, which are I/O intensive and
     * could take long time to process due to disk contentions. The number of threads used for
     * handling finalizeShuffleMerge requests are percentage of io.serverThreads (if defined) else it
     * is a percentage of 2 * #cores.
     */
    public int finalizeShuffleMergeHandlerThreads() {
        if (!this.getModuleName().equalsIgnoreCase("shuffle")) {
            return 0;
        }
        Preconditions.checkArgument(
                separateFinalizeShuffleMerge(),
                "Please set morax.shuffle.server.finalizeShuffleMergeThreadsPercent to a positive value");
        int finalizeShuffleMergeThreadsPercent =
                Integer.parseInt(conf.get("morax.shuffle.server.finalizeShuffleMergeThreadsPercent"));
        int threads = this.serverThreads() > 0 ? this.serverThreads() : 2 * NettyRuntime.availableProcessors();
        return (int) Math.ceil(threads * (finalizeShuffleMergeThreadsPercent / 100.0));
    }

    /**
     * Whether to use a separate EventLoopGroup to process FinalizeShuffleMerge messages, it is
     * decided by the config `morax.shuffle.server.finalizeShuffleMergeThreadsPercent` is set or not.
     */
    public boolean separateFinalizeShuffleMerge() {
        return conf.getInt("morax.shuffle.server.finalizeShuffleMergeThreadsPercent", 0) > 0;
    }

    /**
     * Whether to use the old protocol while doing the shuffle block fetching.
     * It is only enabled while we need the compatibility in the scenario of new morax version
     * job fetching blocks from old version external shuffle service.
     */
    public boolean useOldFetchProtocol() {
        return conf.getBoolean("morax.shuffle.useOldFetchProtocol", false);
    }

    /** Whether to enable sasl retries or not. The number of retries is dictated by the config
     * `morax.shuffle.io.maxRetries`.
     */
    public boolean enableSaslRetries() {
        return conf.getBoolean("morax.shuffle.sasl.enableRetries", false);
    }

    /**
     * The maximum size of cache in memory which is used in push-based shuffle for storing merged
     * index files. This cache is in addition to the one configured via
     * morax.shuffle.service.index.cache.size.
     */
    public long mergedIndexCacheSize() {
        return JavaUtils.byteStringAsBytes(conf.get("morax.shuffle.push.server.mergedIndexCacheSize", "100m"));
    }

    /**
     * The threshold for number of IOExceptions while merging shuffle blocks to a shuffle partition.
     * When the number of IOExceptions while writing to merged shuffle data/index/meta file exceed
     * this threshold then the shuffle server will respond back to client to stop pushing shuffle
     * blocks for this shuffle partition.
     */
    public int ioExceptionsThresholdDuringMerge() {
        return conf.getInt("morax.shuffle.push.server.ioExceptionsThresholdDuringMerge", 4);
    }

    /**
     * The RemoteBlockPushResolver#mergedShuffleCleanermergedShuffleCleaner
     * shutdown timeout, in seconds.
     */
    public long mergedShuffleCleanerShutdownTimeout() {
        return JavaUtils.timeStringAsSec(
                conf.get("morax.shuffle.push.server.mergedShuffleCleaner.shutdown.timeout", "60s"));
    }
}
