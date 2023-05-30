package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class EndpointCollectionTest {

    @Test
    public void shouldEnrichEndpointsUsingFilter() {

        Endpoint endpoint1 = new DatabaseEndpoint().withAddress("address1");
        Endpoint endpoint2 = new DatabaseEndpoint().withAddress("address2");

        EndpointFilter filter = new EndpointFilter() {
            public Endpoint enrichEndpoint(Endpoint endpoint) {
                endpoint.setAnnotation("test", endpoint.getAddress());
                return endpoint;
            }
        };

        EndpointCollection endpoints = new EndpointCollection(Arrays.asList(endpoint1, endpoint2));

        assertFalse(endpoints.get("address1").getAnnotations().containsKey("test"));
        assertFalse(endpoints.get("address2").getAnnotations().containsKey("test"));

        EndpointCollection enrichedEndpoints = endpoints.getEnrichedEndpoints(filter);

        assertTrue(enrichedEndpoints.get("address1").getAnnotations().containsKey("test"));
        assertTrue(enrichedEndpoints.get("address2").getAnnotations().containsKey("test"));

        assertEquals(endpoint1.getAddress(), enrichedEndpoints.get("address1").getAnnotations().get("test"));
        assertEquals(endpoint2.getAddress(), enrichedEndpoints.get("address2").getAnnotations().get("test"));

    }

    @Test
    public void shouldFilterForAcceptedEndpoints(){

        Endpoint endpoint1 = new DatabaseEndpoint().withAddress("address1");
        Endpoint endpoint2 = new DatabaseEndpoint().withAddress("address2");

        EndpointFilter filter = new EndpointFilter() {
            public ApprovalResult approveEndpoint(Endpoint endpoint) {
                return new ApprovalResult(endpoint.getAddress().equals("address1"), null);
            }
        };

        EndpointCollection endpoints = new EndpointCollection(Arrays.asList(endpoint1, endpoint2));

        EndpointCollection acceptedEndpoints = endpoints.getAcceptedEndpoints(filter);

        assertEquals(1, acceptedEndpoints.size());
        assertEquals(endpoint1, acceptedEndpoints.get("address1"));

    }

    @Test
    public void shouldFilterForRejectedEndpoints(){

        Endpoint endpoint1 = new DatabaseEndpoint().withAddress("address1");
        Endpoint endpoint2 = new DatabaseEndpoint().withAddress("address2");

        EndpointFilter filter = new EndpointFilter() {
            public ApprovalResult approveEndpoint(Endpoint endpoint) {
                return new ApprovalResult(endpoint.getAddress().equals("address1"), null);
            }
        };

        EndpointCollection endpoints = new EndpointCollection(Arrays.asList(endpoint1, endpoint2));

        EndpointCollection rejectedEndpoints = endpoints.getRejectedEndpoints(filter);

        assertEquals(1, rejectedEndpoints.size());
        assertEquals(endpoint2, rejectedEndpoints.get("address2"));

    }

    @Test
    public void shouldIdentifyEndpointsForWhichThereIsNoCluster(){

        Endpoint endpoint1 = new DatabaseEndpoint().withAddress("address1");
        Endpoint endpoint2 = new DatabaseEndpoint().withAddress("address2");
        Endpoint endpoint3 = new DatabaseEndpoint().withAddress("address3");

        EndpointCollection endpoints = new EndpointCollection(Arrays.asList(endpoint1, endpoint2, endpoint3));

        Cluster cluster = Cluster.build().create();

        ClusterFactory clusterFactory = e -> cluster;

        ClientClusterCollection clientClusterCollection = new ClientClusterCollection(clusterFactory, null);
        clientClusterCollection.createClusterForEndpoint(endpoint1);

        EndpointCollection endpointsWithNoCluster = endpoints.getEndpointsWithNoCluster(clientClusterCollection);

        assertEquals(2, endpointsWithNoCluster.size());
        assertEquals(endpoint2, endpointsWithNoCluster.get("address2"));
        assertEquals(endpoint3, endpointsWithNoCluster.get("address3"));

        cluster.close();
    }

}