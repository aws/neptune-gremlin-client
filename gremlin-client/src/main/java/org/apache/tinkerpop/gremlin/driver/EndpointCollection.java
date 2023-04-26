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

    private final Map<String, Endpoint> endpoints = new HashMap<>();

    public EndpointCollection() {
    }

    public EndpointCollection(Collection<? extends Endpoint> endpoints) {
        for (Endpoint endpoint : endpoints) {
            addOrReplace(endpoint);
        }
    }

    public void addOrReplace(Endpoint endpoint) {
        endpoints.put(computeKey(endpoint), endpoint);
    }


    public boolean containsAddress(String address) {
        return endpoints.containsKey(address);
    }

    public Collection<String> addresses() {
        return endpoints.keySet();
    }

    public Collection<Endpoint> endpoints() {
        return endpoints.values();
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

    @Override
    public String toString() {
        return "EndpointCollection{" +
                "endpoints=" + endpoints +
                '}';
    }

    private String computeKey(Endpoint endpoint) {
        return endpoint.getAddress() != null ? endpoint.getAddress() : String.valueOf(endpoint.hashCode());
    }
}
