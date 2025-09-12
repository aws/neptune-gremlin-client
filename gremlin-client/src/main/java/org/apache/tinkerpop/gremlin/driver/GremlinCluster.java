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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.GitProperties;
import software.amazon.utils.SoftwareVersion;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class GremlinCluster implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GremlinCluster.class);

    private final Collection<Endpoint> defaultEndpoints;
    private final ClusterFactory clusterFactory;
    private final Collection<ClientClusterCollection> clientClusterCollections = new CopyOnWriteArrayList<>();
    private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);
    private final EndpointStrategies endpointStrategies;
    private final AcquireConnectionConfig acquireConnectionConfig;

    private final MetricsConfig metricsConfig;


    public GremlinCluster(Collection<Endpoint> defaultEndpoints,
                          ClusterFactory clusterFactory,
                          EndpointStrategies endpointStrategies,
                          AcquireConnectionConfig acquireConnectionConfig,
                          MetricsConfig metricsConfig) {
        logger.info("Version: {} {}", SoftwareVersion.FromResource, GitProperties.FromResource);
        logger.info("Created GremlinCluster [defaultEndpoints: {}, enableMetrics: {}]",
                defaultEndpoints.stream()
                        .map(Endpoint::getAddress)
                        .collect(Collectors.toList()),
                metricsConfig.enableMetrics());
        this.defaultEndpoints = defaultEndpoints;
        this.clusterFactory = clusterFactory;
        this.endpointStrategies = endpointStrategies;
        this.acquireConnectionConfig = acquireConnectionConfig;
        this.metricsConfig = metricsConfig;
    }

    public GremlinClient connect(List<String> addresses, Client.Settings settings) {

        return connectToEndpoints(
                addresses.stream()
                        .map(a -> new DatabaseEndpoint().withAddress(a))
                        .collect(Collectors.toList()),
                settings);
    }

    public GremlinClient connectToEndpoints(Collection<Endpoint> endpoints, Client.Settings settings) {

        logger.info("Connecting with: {}", endpoints.stream()
                .map(Endpoint::getAddress)
                .collect(Collectors.toList()));

        if (endpoints.isEmpty()) {
            throw new IllegalStateException("You must supply at least one endpoint");
        }

        Cluster parentCluster = clusterFactory.createCluster(null);

        ClientClusterCollection clientClusterCollection = new ClientClusterCollection(clusterFactory, parentCluster);

        Map<Endpoint, Cluster> clustersForEndpoints = clientClusterCollection.createClustersForEndpoints(new EndpointCollection(endpoints));
        List<EndpointClient> newEndpointClients = EndpointClient.create(clustersForEndpoints);
        // Some of the clients could have been rejected when the connection is being created
        // So they should be added to the rejected list.
        final Set<Endpoint> rejectedEndpoints =  new HashSet<>(clustersForEndpoints.keySet());
        newEndpointClients.forEach(i -> rejectedEndpoints.remove(i.endpoint()));

        final EndpointCollection rejectedEndpointsCollection = new EndpointCollection( rejectedEndpoints);
        clientClusterCollection.removeClusterWithMatchingEndpoint(rejectedEndpointsCollection);
        // validate atleast one endpoint is available otherwise fail !

        if (newEndpointClients.isEmpty()) {
            throw new IllegalStateException("None of the endpoints supplied are available !");
        }

        EndpointClientCollection endpointClientCollection = new EndpointClientCollection(
                EndpointClientCollection.builder()
                        .withEndpointClients(newEndpointClients)
                        .withRejectedEndpoints(rejectedEndpointsCollection)
                        .setCollectMetrics(metricsConfig.enableMetrics()));

        clientClusterCollections.add(clientClusterCollection);

        return new GremlinClient(
                clientClusterCollection.getParentCluster(),
                settings,
                endpointClientCollection,
                clientClusterCollection,
                endpointStrategies,
                acquireConnectionConfig,
                metricsConfig
        );
    }

    public GremlinClient connect(List<String> addresses) {
        return connect(addresses, Client.Settings.build().create());
    }

    public GremlinClient connectToEndpoints(List<Endpoint> endpoints) {
        return connectToEndpoints(endpoints, Client.Settings.build().create());
    }

    public GremlinClient connect() {
        return connectToEndpoints(defaultEndpoints, Client.Settings.build().create());
    }

    public GremlinClient connect(Client.Settings settings) {
        return connectToEndpoints(defaultEndpoints, settings);
    }

    public CompletableFuture<Void> closeAsync() {

        if (closing.get() != null)
            return closing.get();

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ClientClusterCollection clientClusterCollection : clientClusterCollections) {
            futures.add(clientClusterCollection.closeAsync());
        }

        closing.set(CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})));

        return closing.get();
    }

    @Override
    public void close() throws Exception {
        closeAsync().join();
    }
}
