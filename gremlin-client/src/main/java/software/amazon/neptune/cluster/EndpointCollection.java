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

package software.amazon.neptune.cluster;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EndpointCollection implements Iterable<Endpoint> {

    public static EndpointCollection fromAddresses(Collection<String> addresses) {
        EndpointCollection endpoints = new EndpointCollection();
        for (String address : addresses) {
            endpoints.addOrReplace(new DatabaseEndpoint().withEndpoint(address));
        }
        return endpoints;
    }

    private final Map<String, Endpoint> endpointMetadata = new HashMap<>();

    public EndpointCollection() {
    }

    public EndpointCollection(Collection<? extends Endpoint> endpoints) {
        for (Endpoint endpoint : endpoints) {
            addOrReplace(endpoint);
        }
    }

    public void addOrReplace(Endpoint endpoint) {
        endpointMetadata.put(computeKey(endpoint), endpoint);
    }


    public boolean containsAddress(String address) {
        return endpointMetadata.containsKey(address);
    }

    public Collection<String> addresses() {
        return endpointMetadata.keySet();
    }

    public Collection<Endpoint> endpoints() {
        return endpointMetadata.values();
    }

    public Endpoint get(String address) {
        return endpointMetadata.get(address);
    }

    public boolean isEmpty() {
        return endpointMetadata.isEmpty();
    }

    @Override
    public Iterator<Endpoint> iterator() {
        return endpointMetadata.values().iterator();
    }

    @Override
    public String toString() {
        return "EndpointMetadataCollection{" +
                "endpointMetadata=" + endpointMetadata +
                '}';
    }

    private String computeKey(Endpoint endpoint) {
        return endpoint.getEndpoint() != null ? endpoint.getEndpoint() : String.valueOf(endpoint.hashCode());
    }
}
