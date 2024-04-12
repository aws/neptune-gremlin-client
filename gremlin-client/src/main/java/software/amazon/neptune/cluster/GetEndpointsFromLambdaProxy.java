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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.amazonaws.services.lambda.model.TooManyRequestsException;
import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.evanlennick.retry4j.exception.UnexpectedException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.EndpointCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.RegionUtils;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class GetEndpointsFromLambdaProxy implements ClusterEndpointsFetchStrategy, ClusterMetadataSupplier {

    private static final Logger logger = LoggerFactory.getLogger(GetEndpointsFromLambdaProxy.class);

    private static final long FIFTEEN_SECONDS = 15000;

    private final ClusterEndpointsFetchStrategy innerStrategy;
    private final String lambdaName;
    private final AWSLambda lambdaClient;
    private final RetryConfig retryConfig;
    private final AtomicReference<NeptuneClusterMetadata> cachedClusterMetadata = new AtomicReference<>();
    private final AtomicLong lastRefreshTime = new AtomicLong(System.currentTimeMillis());

    GetEndpointsFromLambdaProxy(String lambdaName) {
        this(lambdaName, RegionUtils.getCurrentRegionName());
    }


    GetEndpointsFromLambdaProxy(String lambdaName, String region) {
        this(lambdaName, region, IamAuthConfig.DEFAULT_PROFILE);
    }


    GetEndpointsFromLambdaProxy(String lambdaName, String region, String iamProfile) {
        this(lambdaName, region, iamProfile, null, null);
    }

    GetEndpointsFromLambdaProxy(String lambdaName, String region, String iamProfile, ClientConfiguration clientConfiguration) {
        this(lambdaName, region, iamProfile, null, clientConfiguration);
    }


    GetEndpointsFromLambdaProxy(String lambdaName, String region, AWSCredentialsProvider credentials) {
        this(lambdaName, region, IamAuthConfig.DEFAULT_PROFILE, credentials, null);
    }

    GetEndpointsFromLambdaProxy(String lambdaName, String region, AWSCredentialsProvider credentials, ClientConfiguration clientConfiguration) {
        this(lambdaName, region, IamAuthConfig.DEFAULT_PROFILE, credentials, clientConfiguration);
    }

    private GetEndpointsFromLambdaProxy(String lambdaName,
                                        String region,
                                        String iamProfile,
                                        AWSCredentialsProvider credentials,
                                        ClientConfiguration clientConfiguration) {
        this.innerStrategy = new CommonClusterEndpointsFetchStrategy(this);
        this.lambdaName = lambdaName;
        this.lambdaClient = createLambdaClient(region, iamProfile, credentials, clientConfiguration);
        this.retryConfig = new RetryConfigBuilder()
                .retryOnSpecificExceptions(TooManyRequestsException.class, TimeoutException.class)
                .withMaxNumberOfTries(5)
                .withDelayBetweenTries(100, ChronoUnit.MILLIS)
                .withExponentialBackoff()
                .build();
    }

    @Override
    public ClusterMetadataSupplier clusterMetadataSupplier() {
        return this;
    }

    @Override
    public NeptuneClusterMetadata refreshClusterMetadata() {

        Callable<NeptuneClusterMetadata> query = () -> {

            InvokeRequest invokeRequest = new InvokeRequest()
                    .withFunctionName(lambdaName)
                    .withPayload("\"\"");
            InvokeResult result = lambdaClient.invoke(invokeRequest);

            if (StringUtils.isNotEmpty(result.getFunctionError())){
                String payload = new String(result.getPayload().array());
                if (payload.contains("Task timed out after")){
                    throw new TimeoutException(String.format("Lambda proxy invocation timed out. Last error message: %s", payload));
                } else {
                    throw new RuntimeException(String.format("Unexpected error while invoking Lambda proxy: %s", payload));
                }
            }

            return NeptuneClusterMetadata.fromByteArray(result.getPayload().array());
        };

        @SuppressWarnings("unchecked")
        CallExecutor<NeptuneClusterMetadata> executor =
                new CallExecutorBuilder<NeptuneClusterMetadata>().config(retryConfig).build();

        Status<NeptuneClusterMetadata> status;

        try {
            status = executor.execute(query);
        } catch (UnexpectedException e) {
            if (e.getCause() instanceof MismatchedInputException) {
                throw new IllegalStateException(String.format("The AWS Lambda proxy (%s) isn't returning a NeptuneClusterMetadata JSON document. Check that the function supports returning a NeptuneClusterMetadata JSON document.", lambdaName), e.getCause());
            } else {
                throw new IllegalStateException(String.format("There was an unexpected error while attempting to get a NeptuneClusterMetadata JSON document from the AWS Lambda proxy (%s). Check that the function supports returning a NeptuneClusterMetadata JSON document.", lambdaName), e.getCause());
            }
        }

        NeptuneClusterMetadata clusterMetadata = status.getResult();

        cachedClusterMetadata.set(clusterMetadata);

        logger.debug("clusterMetadata: {}", clusterMetadata);

        return clusterMetadata;
    }

    @Override
    public NeptuneClusterMetadata getClusterMetadata() {
        NeptuneClusterMetadata clusterMetadata = cachedClusterMetadata.get();
        if (clusterMetadata == null) {
            return refreshClusterMetadata();
        }
        if (shouldRefresh()){
            return refreshClusterMetadata();
        }
        return clusterMetadata;
    }

    @Override
    public Map<? extends EndpointsSelector, EndpointCollection> getEndpoints(Collection<? extends EndpointsSelector> selectors, boolean refresh) {
        return innerStrategy.getEndpoints(selectors, refresh);
    }

    private boolean shouldRefresh() {
        // Ensure cached values are refreshed every 5 seconds
        final long now = System.currentTimeMillis();
        long refreshTime = lastRefreshTime.updateAndGet(currentValue -> now - currentValue > FIFTEEN_SECONDS ? now : currentValue);
        return (refreshTime == now);
    }

    private AWSLambda createLambdaClient(String region, String iamProfile, AWSCredentialsProvider credentials, ClientConfiguration clientConfiguration) {

        AWSLambdaClientBuilder builder = AWSLambdaClientBuilder.standard();

        if (clientConfiguration != null){
            builder = builder.withClientConfiguration(clientConfiguration);
        }

        if (credentials != null) {
            builder = builder.withCredentials(credentials);
        } else {

            if (!iamProfile.equals(IamAuthConfig.DEFAULT_PROFILE)) {
                builder = builder.withCredentials(new ProfileCredentialsProvider(iamProfile));
            } else {
                builder = builder.withCredentials(DefaultAWSCredentialsProviderChain.getInstance());
            }
        }

        if (StringUtils.isNotEmpty(region)) {
            builder = builder.withRegion(region);
        }

        return builder.build();
    }
}
