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
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GremlinClient extends Client implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GremlinClient.class);

    private final AtomicReference<ClientHolderCollection> clients = new AtomicReference<>(new ClientHolderCollection());
    private final AtomicLong index = new AtomicLong(0);
    private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);
    private final ConnectionAttemptManager connectionAttemptManager;
    private final GremlinClusterCollection clusterCollection;
    private final Function<Collection<String>, Cluster> clusterBuilder;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final EndpointStrategies endpointStrategies;
    private final AcquireConnectionConfig acquireConnectionConfig;

    GremlinClient(Cluster cluster,
                  Settings settings,
                  ClientHolderCollection clients,
                  GremlinClusterCollection clusterCollection,
                  Function<Collection<String>, Cluster> clusterBuilder,
                  EndpointStrategies endpointStrategies,
                  AcquireConnectionConfig acquireConnectionConfig) {
        super(cluster, settings);

        this.clients.set(clients);
        this.clusterCollection = clusterCollection;
        this.clusterBuilder = clusterBuilder;
        this.endpointStrategies = endpointStrategies;
        this.acquireConnectionConfig = acquireConnectionConfig;
        this.connectionAttemptManager = acquireConnectionConfig.createConnectionAttemptManager(this);

        logger.info("availableEndpointFilter: {}", endpointStrategies.availableEndpointFilter());
    }

    /**
     * Refreshes the list of endpoint addresses to which the client connects.
     */
    public void refreshEndpoints(String... addresses) {
        refreshEndpoints(EndpointCollection.fromAddresses(Arrays.asList(addresses)));
    }

    /**
     * Refreshes the list of endpoint addresses to which the client connects.
     */
    public synchronized void refreshEndpoints(EndpointCollection endpoints) {

        if (closing.get() != null) {
            return;
        }

        AvailableEndpointFilter endpointFilter =
                new EmptyEndpointFilter(endpointStrategies.availableEndpointFilter());

        EndpointCollection acceptedEndpoints = new EndpointCollection();
        EndpointCollection rejectedEndpoints = new EndpointCollection();

        for (Endpoint endpoint : endpoints) {
            endpoint = endpointFilter.enrichEndpoint(endpoint);
            ApprovalResult approvalResult = endpointFilter.approveEndpoint(endpoint);
            if (approvalResult.isApproved()) {
                acceptedEndpoints.addOrReplace(endpoint);
            } else {
                rejectedEndpoints.addOrReplace(approvalResult.enrich(endpoint));
            }
        }

        ClientHolderCollection oldClientHolders = clients.get();
        ClientHolderCollection newClientHolders = new ClientHolderCollection(acceptedEndpoints, rejectedEndpoints);
        List<String> addressesToRemove = new ArrayList<>();

        for (ClientHolder clientHolder : oldClientHolders) {
            String address = clientHolder.getEndpoint();
            if (acceptedEndpoints.containsAddress(address)) {
                logger.info("Retaining client for {}", address);
                newClientHolders.add(clientHolder);
            } else {
                addressesToRemove.add(address);
            }
        }

        for (Endpoint endpoint : acceptedEndpoints) {
            String address = endpoint.getEndpoint();
            if (!clusterCollection.containsAddress(address)) {
                logger.info("Adding client for {}", address);
                Cluster cluster = clusterBuilder.apply(Collections.singletonList(address));
                ClientHolder clientHolder = new ClientHolder(endpoint.getEndpoint(), cluster.connect());
                clientHolder.init();
                newClientHolders.add(clientHolder);
                clusterCollection.add(address, cluster);
            }
        }

        clients.set(newClientHolders);

        for (String address : addressesToRemove) {
            logger.info("Removing client for {}", address);
            Cluster cluster = clusterCollection.remove(address);
            if (cluster != null) {
                cluster.close();
            }
        }
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

            ClientHolderCollection currentClientHolders = clients.get();

            while (currentClientHolders.isEmpty()) {

                if (connectionAttemptManager.maxWaitTimeExceeded(start)) {
                    if (currentClientHolders.hasRejectedEndpoints()) {
                        throw new EndpointsUnavailableException(currentClientHolders.rejectionReasons());
                    } else {
                        throw new TimeoutException("Timed-out waiting for connection");
                    }
                }

                if (connectionAttemptManager.eagerRefreshWaitTimeExceeded(start)) {
                    connectionAttemptManager.triggerEagerRefresh(new EagerRefreshContext());
                }

                try {
                    Thread.sleep(acquireConnectionConfig.acquireConnectionBackoffMillis());
                    currentClientHolders = clients.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            ClientHolder clientHolder = currentClientHolders.get((int) (index.getAndIncrement() % currentClientHolders.size()));

            connection = clientHolder.chooseConnection(msg);

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
        return new GremlinAliasClusterClient(this, aliases, settings, clusterCollection);
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
        executorService.shutdownNow();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ClientHolder clientHolder : clients.get()) {
            futures.add(clientHolder.closeAsync());
        }

        closing.set(CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})));

        return closing.get();
    }

    @Override
    public synchronized Client init() {
        if (initialized)
            return this;

        logger.debug("Initializing internal clients");

        for (ClientHolder clientHolder : clients.get()) {
            clientHolder.init();
        }
        initializeImplementation();

        initialized = true;
        return this;
    }

    @Override
    public String toString() {

        return "Client holder queue: " + System.lineSeparator() +
                clients.get().stream()
                        .map(c -> String.format("  {endpoint: %s, isAvailable: %s}",
                                c.getEndpoint(),
                                c.isAvailable()))
                        .collect(Collectors.joining(System.lineSeparator())) +
                System.lineSeparator() +
                "Cluster collection: " + System.lineSeparator() +
                clusterCollection.toString();
    }

    public static class GremlinAliasClusterClient extends AliasClusteredClient {

        private final GremlinClusterCollection clusterCollection;

        GremlinAliasClusterClient(Client client, Map<String, String> aliases, Settings settings, GremlinClusterCollection clusterCollection) {
            super(client, aliases, settings);
            this.clusterCollection = clusterCollection;
        }

        @Override
        public Cluster getCluster() {
            Cluster cluster = clusterCollection.getFirstOrNull();
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
