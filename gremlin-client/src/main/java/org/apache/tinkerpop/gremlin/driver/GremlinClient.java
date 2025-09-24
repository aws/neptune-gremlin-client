/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.cluster.ClusterEndpointsRefreshAgent;
import software.amazon.utils.CollectionUtils;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class GremlinClient extends Client implements Refreshable, AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GremlinClient.class);

    private final AtomicReference<EndpointClientCollection> endpointClientCollection = new AtomicReference<>(new EndpointClientCollection());
    private final AtomicLong index = new AtomicLong(0);
    private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);
    private final ConnectionAttemptManager connectionAttemptManager;
    private final ClientClusterCollection clientClusterCollection;
    private final EndpointStrategies endpointStrategies;
    private final AcquireConnectionConfig acquireConnectionConfig;
    private final MetricsConfig metricsConfig;

    GremlinClient(Cluster cluster,
                  Settings settings,
                  EndpointClientCollection endpointClientCollection,
                  ClientClusterCollection clientClusterCollection,
                  EndpointStrategies endpointStrategies,
                  AcquireConnectionConfig acquireConnectionConfig,
                  MetricsConfig metricsConfig) {
        super(cluster, settings);

        this.endpointClientCollection.set(endpointClientCollection);
        this.clientClusterCollection = clientClusterCollection;
        this.endpointStrategies = endpointStrategies;
        this.acquireConnectionConfig = acquireConnectionConfig;
        this.connectionAttemptManager = acquireConnectionConfig.createConnectionAttemptManager(this);
        this.metricsConfig = metricsConfig;

        logger.info("availableEndpointFilter: {}", endpointStrategies.endpointFilter());
    }

    /**
     * Refreshes the client with its current set of endpoints.
     * (Useful for triggering metrics for static cluster topologies.)
     */
    public void refreshEndpoints(){
        refreshEndpoints(currentEndpoints());
    }

    /**
     * Refreshes the list of endpoint addresses to which the client connects.
     */
    public void refreshEndpoints(Collection<? extends Endpoint> endpoints) {
        refreshEndpoints(new EndpointCollection(endpoints));
    }

    /**
     * Refreshes the list of endpoint addresses to which the client connects.
     */
    public void refreshEndpoints(Endpoint... endpoints) {
        refreshEndpoints(new EndpointCollection(endpoints));
    }

    /**
     * Refreshes the list of endpoint addresses to which the client connects.
     */
    @Override
    public synchronized void refreshEndpoints(EndpointCollection endpoints) {

        if (closing.get() != null) {
            return;
        }

        EndpointFilter endpointFilter =
                new EmptyEndpointFilter(endpointStrategies.endpointFilter());

        EndpointClientCollection currentEndpointClientCollection = endpointClientCollection.get();

        EndpointCollection enrichedEndpoints = endpoints.getEnrichedEndpoints(endpointFilter);

        EndpointCollection acceptedEndpoints = enrichedEndpoints.getAcceptedEndpoints(endpointFilter);
        EndpointCollection rejectedEndpoints = enrichedEndpoints.getRejectedEndpoints(endpointFilter);

        List<EndpointClient> survivingEndpointClients =
                currentEndpointClientCollection.getSurvivingEndpointClients(acceptedEndpoints);

        EndpointCollection newEndpoints = acceptedEndpoints.getEndpointsWithNoCluster(clientClusterCollection);
        Map<Endpoint, Cluster> newEndpointClusters = clientClusterCollection.createClustersForEndpoints(newEndpoints);
        List<EndpointClient> newEndpointClients = EndpointClient.create(newEndpointClusters);

        EndpointClientCollection newEndpointClientCollection = new EndpointClientCollection(
                EndpointClientCollection.builder()
                        .withEndpointClients(CollectionUtils.join(survivingEndpointClients, newEndpointClients))
                        .withRejectedEndpoints(rejectedEndpoints)
                        .setCollectMetrics(metricsConfig.enableMetrics())
        );

        endpointClientCollection.set(newEndpointClientCollection);
        clientClusterCollection.removeClustersWithNoMatchingEndpoint(newEndpointClientCollection.endpoints());

        currentEndpointClientCollection.close(metricsConfig.metricsHandlers());
    }

    public EndpointCollection currentEndpoints(){
        return endpointClientCollection.get().endpoints();
    }

    @Override
    protected void initializeImplementation() {
        // Do nothing
    }

    @Override
    protected Connection chooseConnection(RequestMessage msg) throws TimeoutException, ConnectionException {

        long start = System.currentTimeMillis();

        logger.debug("Choosing connection");

        Connection connection = null;

        while (connection == null) {

            EndpointClientCollection currentEndpointClientCollection = endpointClientCollection.get();

            while (currentEndpointClientCollection.isEmpty()) {

                if (connectionAttemptManager.maxWaitTimeExceeded(start)) {
                    if (currentEndpointClientCollection.hasRejectedEndpoints()) {
                        throw new EndpointsUnavailableException(currentEndpointClientCollection.rejectionReasons());
                    } else {
                        throw new TimeoutException("Timed-out waiting for connection");
                    }
                }

                if (connectionAttemptManager.eagerRefreshWaitTimeExceeded(start)) {
                    connectionAttemptManager.triggerEagerRefresh(new EagerRefreshContext());
                }

                try {
                    Thread.sleep(acquireConnectionConfig.acquireConnectionBackoffMillis());
                    currentEndpointClientCollection = endpointClientCollection.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            connection = currentEndpointClientCollection.chooseConnection(
                    msg,
                    ec -> {
                        int retryCount = 0;
                        while(retryCount < ec.size()) {
                            final EndpointClient endpointClient = ec.get((int) (index.getAndIncrement() % ec.size()));
                            if (!endpointClient.isAvailable()) {
                                logger.debug("Strategy: No connections available for {}", endpointClient.endpoint().getAddress());
                                retryCount++;
                                continue;
                            }
                            return endpointClient;
                        }
                        throw new EndpointsUnavailableException(List.of("None of the existing clients have connections available to be used."));
                    });

            if (connection == null) {

                if (connectionAttemptManager.maxWaitTimeExceeded(start)) {
                    throw new TimeoutException("Timed-out waiting for connection");
                }

                if (connectionAttemptManager.eagerRefreshWaitTimeExceeded(start)) {
                    connectionAttemptManager.triggerEagerRefresh(new EagerRefreshContext());
                }

                try {
                    Thread.sleep(acquireConnectionConfig.acquireConnectionBackoffMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        logger.debug("Connection: {} [{} ms]", connection.getConnectionInfo(), System.currentTimeMillis() - start);

        return connection;
    }

    @Override
    public Client alias(String graphOrTraversalSource) {
        return alias(makeDefaultAliasMap(graphOrTraversalSource));
    }

    @Override
    public Client alias(final Map<String, String> aliases) {
        return new GremlinAliasClusterClient(this, aliases, settings, clientClusterCollection, endpointClientCollection);
    }

    @Override
    public boolean isClosing() {
        return closing.get() != null;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {

        if (closing.get() != null)
            return closing.get();

        connectionAttemptManager.shutdownNow();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (EndpointClient endpointClient : endpointClientCollection.get()) {
            futures.add(endpointClient.closeClientAsync());
        }

        closing.set(CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})));

        return closing.get();
    }

    /**
     * For the {@link GremlinClient} wrapper, prefer use of {@link #initEndpointCollection()}.
     * </p>
     * This override of initialization is a bit of a do-nothing. It purposely does not proxy the call to
     * {@code super} or try to call it for the {@link #endpointClientCollection} because it has the potential to
     * cause monitor lock contention with calls to {@code submit}. By relegating initialization to {@code submit}
     * only we avoid the chance for stalls on requests that are in conflict with the
     * {@link ClusterEndpointsRefreshAgent}.
     * <p/>
     * The notion of {@link #initialized} doesn't have much meaning for this implementation as a wrapper, so it too
     * is being ignored.
     */
    @Override
    public synchronized Client init() {
        if (isClosing()) throw new IllegalStateException("Client is closed");
        return this;
    }

    /**
     * This method will initialize the underlying connection pools in each {@link Client} instance. It is important that
     * this method not be called in parallel to making requests with {@code submit}.
     */
    public synchronized void initEndpointCollection() {
        for (EndpointClient endpointClient : endpointClientCollection.get()) {
            endpointClient.initClient();
        }
    }

    @Override
    public String toString() {

        return "Client holder queue: " + System.lineSeparator() +
                endpointClientCollection.get().stream()
                        .map(c -> String.format("  {address: %s, isAvailable: %s}",
                                c.endpoint().getAddress(),
                                !c.client().getCluster().availableHosts().isEmpty()))
                        .collect(Collectors.joining(System.lineSeparator())) +
                System.lineSeparator() +
                "Cluster collection: " + System.lineSeparator() +
                clientClusterCollection.toString();
    }

    public static class GremlinAliasClusterClient extends AliasClusteredClient {

        private static final Logger logger = LoggerFactory.getLogger(GremlinAliasClusterClient.class);

        private final ClientClusterCollection clientClusterCollection;
        private final AtomicReference<EndpointClientCollection> endpointClientCollection;

        GremlinAliasClusterClient(Client client,
                                  Map<String, String> aliases,
                                  Settings settings,
                                  ClientClusterCollection clientClusterCollection,
                                  AtomicReference<EndpointClientCollection> endpointClientCollection) {
            super(client, aliases, settings);
            this.clientClusterCollection = clientClusterCollection;
            this.endpointClientCollection = endpointClientCollection;
        }

        @Override
        public CompletableFuture<ResultSet> submitAsync(Bytecode bytecode, RequestOptions options) {
            long start = System.currentTimeMillis();
            UUID traceId = options.getOverrideRequestId().isPresent() ? options.getOverrideRequestId().get() : UUID.randomUUID();
            logger.trace("_traceId: {}", traceId);
            RequestOptions.Builder newOptions = RequestOptions.build();
            newOptions.overrideRequestId(traceId);

            if (options.getAliases().isPresent()) {
                Map<String, String> aliases = options.getAliases().get();
                for (Map.Entry<String, String> alias : aliases.entrySet()) {
                    newOptions.addAlias(alias.getKey(), alias.getValue());
                }
            }
            if (options.getBatchSize().isPresent()) {
                newOptions.batchSize(options.getBatchSize().get());
            }

            if (options.getTimeout().isPresent()) {
                newOptions.timeout(options.getTimeout().get());
            }
            if (options.getLanguage().isPresent()) {
                newOptions.language(options.getLanguage().get());
            }
            if (options.getUserAgent().isPresent()) {
                newOptions.userAgent(options.getUserAgent().get());
            }
            if (options.getParameters().isPresent()) {
                Map<String, Object> params = options.getParameters().get();
                for (Map.Entry<String, Object> param : params.entrySet()) {
                    newOptions.addParameter(param.getKey(), param.getValue());
                }
            }

            CompletableFuture<ResultSet> future = super.submitAsync(bytecode, newOptions.create());

            EndpointClientCollection endpointClients = endpointClientCollection.get();

            if (endpointClients != null){
                return future.whenComplete((results, throwable) -> {
                    long durationMillis = System.currentTimeMillis() - start;
                    endpointClients.registerDurationForTraceId(traceId, durationMillis, throwable);
                });
            } else {
                return future;
            }

        }

        /**
         * For the {@link GremlinClient} wrapper, prefer use of {@link #initEndpointCollection()}.
         * </p>
         * This override of initialization is a bit of a do-nothing. It purposely does not proxy the call to
         * {@code super} or try to call it for the {@link #endpointClientCollection} because it has the potential to
         * cause monitor lock contention with calls to {@code submit}. By relegating initialization to {@code submit}
         * only we avoid the chance for stalls on requests that are in conflict with the
         * {@link ClusterEndpointsRefreshAgent}.
         * <p/>
         * The notion of {@link #initialized} doesn't have much meaning for this implementation as a wrapper, so it too
         * is being ignored.
         */
        @Override
        public synchronized Client init() {
            if (isClosing()) throw new IllegalStateException("Client is closed");
            return this;
        }

        /**
         * This method will initialize the underlying connection pools in each {@link Client} instance. It is important that
         * this method not be called in parallel to making requests with {@code submit}.
         */
        public synchronized void initEndpointCollection() {
            for (EndpointClient endpointClient : endpointClientCollection.get()) {
                endpointClient.initClient();
            }
        }

        @Override
        public Cluster getCluster() {
            Cluster cluster = clientClusterCollection.getFirstOrNull();
            if (cluster != null) {
                logger.trace("Returning: Cluster: {}, Hosts: [{}}",
                        cluster,
                        cluster.availableHosts().stream().map(URI::toString).collect(Collectors.joining(", ")));
                return cluster;
            } else {
                logger.warn("Unable to find cluster with available hosts in cluster collection, so returning parent cluster, which has no hosts.");
                return super.getCluster();
            }
        }
    }
}
