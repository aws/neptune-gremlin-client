package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.neptune.cluster.SuspendedEndpoints;

public class StatusEndpointFilterTest {

    @Test
    public void testStatusEndpointWithHealthyStatus() throws Exception {
        testSetup("healthy", true);
    }

    @Test
    public void testStatusEndpointWithUnhealthyStatus() throws Exception {
        testSetup("unhealthy", false);
    }

    private void testSetup(final String status, final boolean expectedResult) throws Exception {
        final AwsCredentialsProvider mockCredentials = Mockito.mock(AwsCredentialsProvider.class);
        final StatusEndpointFilter filter = new StatusEndpointFilter(Region.US_EAST_1, mockCredentials);
        final Endpoint endpoint = new DatabaseEndpoint().withAddress("test-endpoint.com");

        endpoint.setAnnotation(SuspendedEndpoints.STATE_ANNOTATION, status);

        final ApprovalResult result = filter.approveEndpoint(endpoint);

        Assert.assertEquals(expectedResult, result.isApproved());
    }
}
