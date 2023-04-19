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
import software.amazon.neptune.cluster.DatabaseEndpoint;
import software.amazon.neptune.cluster.Endpoint;
import software.amazon.neptune.cluster.EndpointCollection;
import software.amazon.neptune.cluster.EndpointStrategies;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class GremlinCluster implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(GremlinCluster.class);

    private final Collection<Endpoint> defaultEndpoints;
    private final Function<Collection<String>, Cluster> clusterBuilder;
    private final Collection<GremlinClusterCollection> clusterCollections = new CopyOnWriteArrayList<>();
    private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);
    private final EndpointStrategies endpointStrategies;


    public GremlinCluster(Collection<Endpoint> defaultEndpoints,
                          Function<Collection<String>, Cluster> clusterBuilder,
                          EndpointStrategies endpointStrategies) {
        logger.info("Created GremlinCluster, defaultEndpoints: {}", defaultEndpoints.stream()
                .map(Endpoint::getEndpoint)
                .collect(Collectors.toList()) );
        this.defaultEndpoints = defaultEndpoints;
        this.clusterBuilder = clusterBuilder;
        this.endpointStrategies = endpointStrategies;
    }

    public GremlinClient connect(List<String> addresses, Client.Settings settings) {

        return connectToEndpoints(
                addresses.stream()
                        .map(a -> new DatabaseEndpoint().withEndpoint(a))
                        .collect(Collectors.toList()),
                settings);
    }

    public GremlinClient connectToEndpoints(Collection<Endpoint> endpoints, Client.Settings settings) {

        logger.info("Connecting with: {}", endpoints.stream()
                .map(Endpoint::getEndpoint)
                .collect(Collectors.toList()));

        if (endpoints.isEmpty()){
            throw new IllegalStateException("You must supply at least one endpoint");
        }

        Cluster parentCluster = clusterBuilder.apply(null);

        GremlinClusterCollection clusterCollection = new GremlinClusterCollection(parentCluster);
        ClientHolderCollection clientHolders = new ClientHolderCollection(new EndpointCollection(endpoints));

        for (Endpoint endpoint : endpoints) {
            Cluster cluster = clusterBuilder.apply(Collections.singletonList(endpoint.getEndpoint()));
            clientHolders.add(new ClientHolder(endpoint.getEndpoint(), cluster.connect()));
            clusterCollection.add(endpoint.getEndpoint(), cluster);
        }

        clusterCollections.add(clusterCollection);

        return new GremlinClient(
                clusterCollection.getParentCluster(),
                settings,
                clientHolders,
                clusterCollection,
                clusterBuilder,
                endpointStrategies
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
        for (GremlinClusterCollection clusterCollection : clusterCollections) {
            futures.add(clusterCollection.closeAsync());
        }

        closing.set(CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})));

        return closing.get();
    }

    @Override
    public void close() throws Exception {
        closeAsync().join();
    }
}