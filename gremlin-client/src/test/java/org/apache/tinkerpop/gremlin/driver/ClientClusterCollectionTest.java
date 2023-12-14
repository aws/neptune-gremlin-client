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

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ClientClusterCollectionTest {

    @Test
    public void shouldCreateClustersForEndpoints() {
        Cluster cluster = Cluster.build().create();
        ClusterFactory clusterFactory = endpoints -> cluster;

        Endpoint endpoint1 = new DatabaseEndpoint().withAddress("address1");
        Endpoint endpoint2 = new DatabaseEndpoint().withAddress("address2");
        Endpoint endpoint3 = new DatabaseEndpoint().withAddress("address3");

        EndpointCollection endpoints = new EndpointCollection(Arrays.asList(endpoint1, endpoint2, endpoint3));

        ClientClusterCollection clientClusterCollection = new ClientClusterCollection(clusterFactory, null);

        Map<Endpoint, Cluster> endpointClusters = clientClusterCollection.createClustersForEndpoints(endpoints);

        assertEquals(3, endpointClusters.size());

        assertTrue(endpointClusters.containsKey(endpoint1));
        assertTrue(endpointClusters.containsKey(endpoint2));
        assertTrue(endpointClusters.containsKey(endpoint3));

        cluster.close();
    }

    @Test
    public void shouldRemoveClustersWithNoMatchingEndpoint() {
        Cluster cluster = Cluster.build().create();

        ClusterFactory clusterFactory = endpoints -> cluster;
        Function<Cluster, Void> clusterCloseMethod = mock(Function.class);

        Endpoint endpoint1 = new DatabaseEndpoint().withAddress("address1");
        Endpoint endpoint2 = new DatabaseEndpoint().withAddress("address2");
        Endpoint endpoint3 = new DatabaseEndpoint().withAddress("address3");
        Endpoint endpoint4 = new DatabaseEndpoint().withAddress("address4");

        EndpointCollection endpoints = new EndpointCollection(Arrays.asList(endpoint1, endpoint2, endpoint3, endpoint4));

        ClientClusterCollection clientClusterCollection = new ClientClusterCollection(clusterFactory, null);
        clientClusterCollection.createClustersForEndpoints(endpoints);


        EndpointCollection survivingEndpoints = new EndpointCollection(Arrays.asList(endpoint1, endpoint3));
        clientClusterCollection.removeClustersWithNoMatchingEndpoint(survivingEndpoints, clusterCloseMethod);

        assertTrue(clientClusterCollection.containsClusterForEndpoint(endpoint1));
        assertTrue(clientClusterCollection.containsClusterForEndpoint(endpoint3));

        assertFalse(clientClusterCollection.containsClusterForEndpoint(endpoint2));
        assertFalse(clientClusterCollection.containsClusterForEndpoint(endpoint4));

        verify(clusterCloseMethod, times(2)).apply(cluster);

        cluster.close();
    }

}