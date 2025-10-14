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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.neptune.auth.credentials.V1toV2CredentialsProvider;
import org.apache.tinkerpop.gremlin.driver.ApprovalResult;
import org.apache.tinkerpop.gremlin.driver.Endpoint;
import org.apache.tinkerpop.gremlin.driver.EndpointFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.neptunedata.NeptunedataClient;
import software.amazon.awssdk.services.neptunedata.model.ClientTimeoutException;
import software.amazon.awssdk.services.neptunedata.model.GetEngineStatusRequest;
import software.amazon.awssdk.services.neptunedata.model.GetEngineStatusResponse;
import software.amazon.neptune.cluster.SuspendedEndpoints;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

public class StatusEndpointFilter implements EndpointFilter {

    private static final Logger logger = LoggerFactory.getLogger(StatusEndpointFilter.class);

    private static final long DEFAULT_TIMEOUT_MILLIS = 2000;
    private static final String STATE_HEALTHY = "healthy";


    private final Region region;
    private final AwsCredentialsProvider credentialsProvider;
    private final long timeoutMillis;

    public StatusEndpointFilter(Region region, AwsCredentialsProvider credentialsProvider, long timeoutMillis) {
        this.region = region;
        this.credentialsProvider = credentialsProvider;
        this.timeoutMillis = timeoutMillis;
    }

    public StatusEndpointFilter(Region region, AwsCredentialsProvider credentialsProvider) {
        this(region, credentialsProvider, DEFAULT_TIMEOUT_MILLIS);
    }

    public StatusEndpointFilter(Region region, AWSCredentialsProvider credentialsProvider, long timeoutMillis) {
        this(region, V1toV2CredentialsProvider.create(credentialsProvider), timeoutMillis);
    }

    public StatusEndpointFilter(Region region, AWSCredentialsProvider credentialsProvider) {
        this(region, credentialsProvider, DEFAULT_TIMEOUT_MILLIS);
    }

    @Override
    public ApprovalResult approveEndpoint(Endpoint endpoint) {
        final Map<String, String> annotations =endpoint.getAnnotations();
        ApprovalResult approvalResult = ApprovalResult.APPROVED;
        if (annotations.containsKey(SuspendedEndpoints.STATE_ANNOTATION) && !(annotations.get(SuspendedEndpoints.STATE_ANNOTATION).equals(STATE_HEALTHY))) {
            approvalResult = new ApprovalResult(false, annotations.get(SuspendedEndpoints.STATE_ANNOTATION));
        }
        logger.info("Approval result: {}", approvalResult);
        return approvalResult;
    }

    @Override
    public Endpoint enrichEndpoint(Endpoint endpoint) {

        final String statusUrl = String.format("https://%s:8182/status", endpoint.getAddress());
        final URI endpointUri = URI.create(statusUrl);

        logger.info("Endpoint URI: {}", endpointUri);

        final ClientOverrideConfiguration configuration = ClientOverrideConfiguration.builder()
                .apiCallTimeout(Duration.ofMillis(timeoutMillis))
                .build();

        final NeptunedataClient client = NeptunedataClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(region)
                .endpointOverride(endpointUri)
                .overrideConfiguration(configuration)
                .build();

        try {

            final GetEngineStatusResponse engineStatus = client.getEngineStatus(GetEngineStatusRequest.builder().build());
            endpoint.setAnnotation(SuspendedEndpoints.STATE_ANNOTATION, engineStatus.status());
            logger.info("Endpoint status: {} [{}]", endpointUri, engineStatus.status());

        } catch (ClientTimeoutException e) {
            endpoint.setAnnotation(SuspendedEndpoints.STATE_ANNOTATION, e.getMessage());
            logger.warn("Timeout while checking endpoint status: {}", endpointUri);
        } catch (Exception e) {
            endpoint.setAnnotation(SuspendedEndpoints.STATE_ANNOTATION, e.getMessage());
            logger.warn("Error while checking endpoint status: {} [{}:{}]", endpointUri, e.getClass().getSimpleName(), e.getMessage());
        } finally {
            client.close();
        }
        return endpoint;
    }
}