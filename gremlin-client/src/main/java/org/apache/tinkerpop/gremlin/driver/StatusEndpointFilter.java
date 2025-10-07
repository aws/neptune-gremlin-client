package org.apache.tinkerpop.gremlin.driver;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.DefaultRequest;
import com.amazonaws.http.HttpMethodName;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public class StatusEndpointFilter implements EndpointFilter {

    private final String region;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public StatusEndpointFilter(String region) {
        this.region = region;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public ApprovalResult approveEndpoint(Endpoint endpoint) {
        try {
            final String statusUrl = String.format("https://%s:8182/status", endpoint.getAddress());
            final URI uri = URI.create(statusUrl);

            final DefaultRequest<Void> awsRequest = new DefaultRequest<>("neptune-db");
            awsRequest.setHttpMethod(HttpMethodName.GET);
            awsRequest.setEndpoint(URI.create(String.format("https://%s:8182", endpoint.getAddress())));
            awsRequest.setResourcePath("/status");

            final AWS4Signer signer = new AWS4Signer();
            signer.setServiceName("neptune-db");
            signer.setRegionName(region);
            signer.sign(awsRequest, DefaultAWSCredentialsProviderChain.getInstance().getCredentials());

            final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder().uri(uri).timeout(Duration.ofSeconds(5)).GET();

            for (final Map.Entry<String, String> header : awsRequest.getHeaders().entrySet()) {
                if (!"Host".equalsIgnoreCase(header.getKey())) {
                    requestBuilder.header(header.getKey(), header.getValue());
                }
            }

            final HttpResponse<String> response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                final JsonNode jsonNode = objectMapper.readTree(response.body());
                if (jsonNode.has("status")) {
                    final boolean healthy = "healthy".equalsIgnoreCase(jsonNode.get("status").asText());
                    return new ApprovalResult(healthy, healthy ? null : "Status not healthy");
                }
                return new ApprovalResult(true, null);
            }
            return new ApprovalResult(false, "HTTP " + response.statusCode());
        } catch (Exception e) {
            return new ApprovalResult(false, e.getMessage());
        }
    }
}