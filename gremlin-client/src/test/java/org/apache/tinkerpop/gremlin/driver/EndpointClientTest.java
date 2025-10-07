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

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class EndpointClientTest {
    @Test
    public void shouldCreateListOfEndpointClientsForEndpointClusters(){
        Cluster cluster = Cluster.build().create();
        Client client = mock(Client.class);

        Endpoint endpoint1 = new DatabaseEndpoint().withAddress("address1");
        Endpoint endpoint2 = new DatabaseEndpoint().withAddress("address2");
        Endpoint endpoint3 = new DatabaseEndpoint().withAddress("address3");

        Map<Endpoint, Cluster> endpointClusters = new HashMap<>();

        endpointClusters.put(endpoint1, cluster);
        endpointClusters.put(endpoint2, cluster);
        endpointClusters.put(endpoint3, cluster);

        List<EndpointClient> endpointClients = EndpointClient.create(endpointClusters, c -> client, new HashMap<>());

        assertEquals(3, endpointClients.size());

        assertTrue(containsEndpointClientWithEndpoint(endpointClients, endpoint1));
        assertTrue(containsEndpointClientWithEndpoint(endpointClients, endpoint2));
        assertTrue(containsEndpointClientWithEndpoint(endpointClients, endpoint3));

        cluster.close();
    }

    private boolean containsEndpointClientWithEndpoint(List<EndpointClient> endpointClients, Endpoint endpoint){
        for (EndpointClient endpointClient : endpointClients) {
            if (endpointClient.endpoint().getAddress().equals(endpoint.getAddress())){
                return true;
            }
        }
        return false;
    }

}