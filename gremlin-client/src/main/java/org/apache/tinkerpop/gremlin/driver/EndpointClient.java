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

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

class EndpointClient {

    public static List<EndpointClient> create(Map<Endpoint, Cluster> endpointClusters){
        return create(endpointClusters, cluster -> cluster.connect().init());
    }

    static List<EndpointClient> create(Map<Endpoint, Cluster> endpointClusters, Function<Cluster, Client> clientFactory){
        List<EndpointClient> results = new ArrayList<>();
        for (Map.Entry<Endpoint, Cluster> entry : endpointClusters.entrySet()) {
            Cluster cluster = entry.getValue();
            Endpoint endpoint = entry.getKey();
            Client client = clientFactory.apply(cluster);

            results.add(new EndpointClient(endpoint, client));
        }
        return results;
    }

    private final Endpoint endpoint;
    private final Client client;

    EndpointClient(Endpoint endpoint, Client client) {
        this.endpoint = endpoint;
        this.client = client;
    }

    public boolean isAvailable() {
        return !client.getCluster().availableHosts().isEmpty();
    }

    public Endpoint endpoint() {
        return endpoint;
    }

    public Client client() {
        return client;
    }

    public void initClient(){
        client.init();
    }

    public CompletableFuture<Void> closeClientAsync(){
        return client.closeAsync();
    }

}
