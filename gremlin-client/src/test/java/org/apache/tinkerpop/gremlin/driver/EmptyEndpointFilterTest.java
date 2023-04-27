package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class EmptyEndpointFilterTest {

    @Test
    public void shouldEnsureThatEndpointsWithNullAddressAreNotAccepted(){

        Endpoint endpoint1 = new DatabaseEndpoint().withAddress("address1");
        Endpoint endpoint2 = new DatabaseEndpoint().withAddress(null);
        Endpoint endpoint3 = new DatabaseEndpoint().withAddress("address3");
        Endpoint endpoint4 = new DatabaseEndpoint().withAddress(null);

        EndpointFilter filter = new EmptyEndpointFilter(EndpointFilter.NULL_ENDPOINT_FILTER);

        EndpointCollection endpoints = new EndpointCollection(
                Arrays.asList(endpoint1, endpoint2, endpoint3, endpoint4));

        EndpointCollection acceptedEndpoints = endpoints.getAcceptedEndpoints(filter);

        assertEquals(2, acceptedEndpoints.size());
        assertEquals(endpoint1, acceptedEndpoints.get("address1"));
        assertEquals(endpoint3, acceptedEndpoints.get("address3"));

        EndpointCollection rejectedEndpoints = endpoints.getRejectedEndpoints(filter);

        assertEquals(2, rejectedEndpoints.size());
        for (Endpoint rejectedEndpoint : rejectedEndpoints) {
            assertEquals("empty", rejectedEndpoint.getAnnotations().get(ApprovalResult.REJECTED_REASON_ANNOTATION));
        }
    }

}