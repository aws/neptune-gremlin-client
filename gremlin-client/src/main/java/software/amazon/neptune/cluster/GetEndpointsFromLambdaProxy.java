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

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.LambdaClientBuilder;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.TooManyRequestsException;
import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.evanlennick.retry4j.exception.UnexpectedException;
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
    private final LambdaClient lambdaClient;
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
        this(lambdaName, region, iamProfile, null, null, null);
    }

    GetEndpointsFromLambdaProxy(String lambdaName, String region, String iamProfile, ClientOverrideConfiguration clientConfiguration) {
        this(lambdaName, region, iamProfile, null, clientConfiguration, null);
    }

    GetEndpointsFromLambdaProxy(String lambdaName, String region, String iamProfile, ClientOverrideConfiguration clientConfiguration, SdkHttpClient.Builder<?> httpClientBuilder) {
        this(lambdaName, region, iamProfile, null, clientConfiguration, httpClientBuilder);
    }

    GetEndpointsFromLambdaProxy(String lambdaName, String region, AwsCredentialsProvider credentials) {
        this(lambdaName, region, IamAuthConfig.DEFAULT_PROFILE, credentials, null, null);
    }

    GetEndpointsFromLambdaProxy(String lambdaName, String region, AwsCredentialsProvider credentials, ClientOverrideConfiguration clientConfiguration) {
        this(lambdaName, region, IamAuthConfig.DEFAULT_PROFILE, credentials, clientConfiguration, null);
    }

    GetEndpointsFromLambdaProxy(String lambdaName, String region, AwsCredentialsProvider credentials, ClientOverrideConfiguration clientConfiguration, SdkHttpClient.Builder<?> httpClientBuilder) {
        this(lambdaName, region, IamAuthConfig.DEFAULT_PROFILE, credentials, clientConfiguration, httpClientBuilder);
    }

    private GetEndpointsFromLambdaProxy(String lambdaName,
                                        String region,
                                        String iamProfile,
                                        AwsCredentialsProvider credentials,
                                        ClientOverrideConfiguration clientOverrideConfiguration,
                                        SdkHttpClient.Builder<?> httpClientBuilder) {
        this.innerStrategy = new CommonClusterEndpointsFetchStrategy(this);
        this.lambdaName = lambdaName;
        this.lambdaClient = createLambdaClient(region, iamProfile, credentials, clientOverrideConfiguration, httpClientBuilder);
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

            InvokeRequest invokeRequest = InvokeRequest.builder()
                    .functionName(lambdaName)
                    .payload(SdkBytes.fromUtf8String("\"\""))
                    .build();
            InvokeResponse result = lambdaClient.invoke(invokeRequest);

            if (StringUtils.isNotEmpty(result.functionError())){
                String payload = result.payload().asUtf8String();
                if (payload.contains("Task timed out after")){
                    throw new TimeoutException(String.format("Lambda proxy invocation timed out. Last error message: %s", payload));
                } else {
                    throw new RuntimeException(String.format("Unexpected error while invoking Lambda proxy: %s", payload));
                }
            }

            return NeptuneClusterMetadata.fromByteArray(result.payload().asByteArray());
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

    private LambdaClient createLambdaClient(String region, String iamProfile, AwsCredentialsProvider credentials, ClientOverrideConfiguration clientConfiguration, SdkHttpClient.Builder<?> httpClientBuilder) {

        LambdaClientBuilder builder = LambdaClient.builder();

        if (clientConfiguration != null){
            builder = builder.overrideConfiguration(clientConfiguration);
        }

        if (credentials != null) {
            builder = builder.credentialsProvider(credentials);
        } else {

            if (!iamProfile.equals(IamAuthConfig.DEFAULT_PROFILE)) {
                builder = builder.credentialsProvider(ProfileCredentialsProvider.create(iamProfile));
            } else {
                builder = builder.credentialsProvider(DefaultCredentialsProvider.create());
            }
        }

        if (StringUtils.isNotEmpty(region)) {
            builder = builder.region(Region.of(region));
        }

        return builder.build();
    }
}
