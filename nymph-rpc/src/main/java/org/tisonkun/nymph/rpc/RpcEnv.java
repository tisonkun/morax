package org.tisonkun.nymph.rpc;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public abstract class RpcEnv {
    private final RpcTimeout defaultLookupTimeout = new RpcTimeout(
            // TODO(@tison) respect NymphConfig
            Duration.ofSeconds(30), "nymph.rpc.lookupTimeout");

    /**
     * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
     * {@link RpcEndpoint#self()}. Return {@code null} if the corresponding {@link RpcEndpointRef}
     * does not exist.
     */
    public abstract RpcEndpointRef endpointRef(RpcEndpoint endpoint);

    /**
     * Return the address that {@link RpcEnv} is listening to.
     */
    public abstract RpcAddress address();

    /**
     * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
     * guarantee thread-safety.
     */
    public abstract RpcEndpointRef setupEndpoint(String name, RpcEndpoint endpoint);

    /**
     * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
     */
    public abstract CompletableFuture<RpcEndpointRef> asyncSetupEndpointRefByURI(String uri);

    /**
     * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
     */
    public final RpcEndpointRef setupEndpointRefByURI(String uri) {
        return defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri));
    }

    /**
     * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
     * This is a blocking action.
     */
    public final RpcEndpointRef setupEndpointRef(RpcAddress address, String endpointName) {
        return setupEndpointRefByURI(new RpcEndpointAddress(address, endpointName).toString());
    }

    /**
     * Stop [[RpcEndpoint]] specified by `endpoint`.
     */
    public abstract void stop(RpcEndpointRef endpointRef);

    /**
     * Shutdown this [[RpcEnv]] asynchronously. If you need to make sure [[RpcEnv]] exits successfully,
     * call [[awaitTermination()]] straight after [[shutdown()]].
     */
    public abstract void shutdown();

    /**
     * Wait until [[RpcEnv]] exits.
     */
    public abstract void awaitTermination();

    /**
     * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
     * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
     */
    public abstract <T> T deserialize(Supplier<T> deserializationAction);
}
