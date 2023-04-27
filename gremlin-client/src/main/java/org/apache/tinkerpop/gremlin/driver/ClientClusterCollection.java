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

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

class ClientClusterCollection {

    private final ClusterFactory clusterFactory;
    private final Cluster parentCluster;
    private final Map<String, Cluster> clusters = new ConcurrentHashMap<>();
    private final AtomicReference<CompletableFuture<Void>> closing = new AtomicReference<>(null);

    private static final Logger logger = LoggerFactory.getLogger(ClientClusterCollection.class);

    ClientClusterCollection(ClusterFactory clusterFactory, Cluster parentCluster) {
        this.clusterFactory = clusterFactory;
        this.parentCluster = parentCluster;
    }

    public Cluster createClusterForEndpoint(Endpoint endpoint){
        Cluster cluster = clusterFactory.createCluster(Collections.singletonList(endpoint.getAddress()));
        clusters.put(endpoint.getAddress(), cluster);
        return cluster;
    }

    public Map<Endpoint, Cluster> createClustersForEndpoints(EndpointCollection endpoints){
        Map<Endpoint, Cluster> results = new HashMap<>();
        for (Endpoint endpoint : endpoints) {
            results.put(endpoint, createClusterForEndpoint(endpoint));
        }
        return results;
    }

    public boolean containsClusterForEndpoint(Endpoint endpoint) {
        return clusters.containsKey(endpoint.getAddress());
    }

    public void removeClustersWithNoMatchingEndpoint(EndpointCollection endpoints){
        removeClustersWithNoMatchingEndpoint(endpoints, cluster -> {
            if (cluster != null){
                cluster.close();
            }
            return null;
        });
    }

    void removeClustersWithNoMatchingEndpoint(EndpointCollection endpoints, Function<Cluster, Void> clusterCloseMethod){
        List<String> removalList = new ArrayList<>();
        for (String address : clusters.keySet()) {
            if (!endpoints.containsEndpoint(new DatabaseEndpoint().withAddress(address))){
                removalList.add(address);
            }
        }
        for (String address : removalList) {
            logger.info("Removing client for {}", address);
            Cluster cluster = clusters.remove(address);
            clusterCloseMethod.apply(cluster);
        }
    }

    public Cluster getParentCluster() {
        return parentCluster;
    }

    public CompletableFuture<Void> closeAsync() {

        if (closing.get() != null)
            return closing.get();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Cluster cluster : clusters.values()) {
            futures.add(cluster.closeAsync());
        }
        futures.add(parentCluster.closeAsync());

        closing.set(CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})));

        return closing.get();
    }

    public Cluster getFirstOrNull(){
        Optional<Map.Entry<String, Cluster>> first = clusters.entrySet().stream().findFirst();
        return first.map(Map.Entry::getValue).orElse(null);
    }

    @Override
    public String toString() {
        return clusters.entrySet().stream()
                .map(e -> String.format("  {%s, %s, isClosed: %s}",
                        e.getKey(),
                        e.getValue().allHosts().stream().map(h -> h.getHostUri().toString()).collect(Collectors.joining(",")),
                        e.getValue().isClosed()))
                .collect(Collectors.joining(System.lineSeparator()));
    }
}
