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

import java.util.*;
import java.util.stream.Stream;

public class EndpointCollection implements Iterable<Endpoint> {

    private final Map<String, Endpoint> endpoints = new HashMap<>();

    public EndpointCollection() {
    }

    public EndpointCollection(Collection<? extends Endpoint> endpoints) {
        for (Endpoint endpoint : endpoints) {
            addOrReplace(endpoint);
        }
    }

    public EndpointCollection getEndpointsWithNoCluster(ClientClusterCollection clientClusterCollection) {
        EndpointCollection results = new EndpointCollection();
        for (Endpoint endpoint : endpoints.values()) {
            if (!clientClusterCollection.containsClusterForEndpoint(endpoint)) {
                results.addOrReplace(endpoint);
            }
        }
        return results;
    }

    public EndpointCollection getEnrichedEndpoints(EndpointFilter endpointFilter) {
        EndpointCollection results = new EndpointCollection();
        for (Endpoint endpoint : endpoints.values()) {
            results.addOrReplace(endpointFilter.enrichEndpoint(endpoint));
        }
        return results;
    }

    public EndpointCollection getAcceptedEndpoints(EndpointFilter endpointFilter) {
        EndpointCollection results = new EndpointCollection();
        for (Endpoint endpoint : endpoints.values()) {
            ApprovalResult approvalResult = endpointFilter.approveEndpoint(endpoint);
            if (approvalResult.isApproved()) {
                results.addOrReplace(endpoint);
            }
        }
        return results;
    }

    public EndpointCollection getRejectedEndpoints(EndpointFilter endpointFilter) {
        EndpointCollection results = new EndpointCollection();
        for (Endpoint endpoint : endpoints.values()) {
            ApprovalResult approvalResult = endpointFilter.approveEndpoint(endpoint);
            if (!approvalResult.isApproved()) {
                results.addOrReplace(approvalResult.enrich(endpoint));
            }
        }
        return results;
    }

    private void addOrReplace(Endpoint endpoint) {
        endpoints.put(computeKey(endpoint), endpoint);
    }

    public boolean containsEndpoint(Endpoint endpoint) {
        return endpoints.containsKey(endpoint.getAddress());
    }

    public Endpoint get(String address) {
        return endpoints.get(address);
    }

    public boolean isEmpty() {
        return endpoints.isEmpty();
    }

    @Override
    public Iterator<Endpoint> iterator() {
        return endpoints.values().iterator();
    }

    public Stream<Endpoint> stream() {
        return endpoints.values().stream();
    }

    @Override
    public String toString() {
        return "EndpointCollection{" +
                "endpoints=" + endpoints +
                '}';
    }

    private String computeKey(Endpoint endpoint) {
        return endpoint.getAddress() != null ? endpoint.getAddress() : String.valueOf(endpoint.hashCode());
    }

    public int size() {
        return endpoints.size();
    }
}
