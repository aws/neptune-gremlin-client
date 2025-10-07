package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;
import org.junit.Assert;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.ArgumentMatchers;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest;

public class StatusEndpointFilterTest {

    private static final String VALID_RESPONSE = "{\"status\":\"healthy\"}";
    private static final String INVALID_RESPONSE = "{\"status\":\"unhealthy\"}";

    @Test
    public void testStatusEndpointWithHealthyStatus() throws Exception {
        testSetup(VALID_RESPONSE, true);
    }

    @Test
    public void testStatusEndpointWithUnhealthyStatus() throws Exception {
        testSetup(INVALID_RESPONSE, false);
    }

    private void testSetup(final String response, final boolean expectedResult) throws Exception {
        final HttpClient mockClient = Mockito.mock(HttpClient.class);
        final HttpResponse<String> mockResponse = (HttpResponse<String>) Mockito.mock(HttpResponse.class);

        Mockito.when(mockResponse.statusCode()).thenReturn(200);
        Mockito.when(mockResponse.body()).thenReturn(response);
        Mockito.when(mockClient.send(ArgumentMatchers.any(HttpRequest.class), ArgumentMatchers.any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

        try (final MockedStatic<HttpClient> mockedStatic = Mockito.mockStatic(HttpClient.class)) {
            final HttpClient.Builder mockBuilder = Mockito.mock(HttpClient.Builder.class);

            Mockito.when(mockBuilder.connectTimeout(ArgumentMatchers.any())).thenReturn(mockBuilder);
            Mockito.when(mockBuilder.build()).thenReturn(mockClient);
            mockedStatic.when(HttpClient::newBuilder).thenReturn(mockBuilder);

            final StatusEndpointFilter filter = new StatusEndpointFilter("us-east-1");
            final Endpoint endpoint = new DatabaseEndpoint().withAddress("test-endpoint.com");

            final ApprovalResult result = filter.approveEndpoint(endpoint);

            Assert.assertEquals(result.isApproved(), expectedResult);
        }
    }
}
