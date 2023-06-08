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

package software.amazon.lambda;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.apache.tinkerpop.gremlin.driver.Endpoint;
import org.apache.tinkerpop.gremlin.driver.EndpointCollection;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.cluster.*;
import software.amazon.utils.EnvironmentVariableUtils;
import software.amazon.utils.RegionUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.charset.StandardCharsets.UTF_8;

public class NeptuneEndpointsInfoLambda implements RequestStreamHandler {

    private static final Logger logger = LoggerFactory.getLogger(NeptuneEndpointsInfoLambda.class);

    private final ClusterEndpointsRefreshAgent refreshAgent;
    private final AtomicReference<NeptuneClusterMetadata> neptuneClusterMetadata = new AtomicReference<>();
    private final String suspendedEndpoints;

    public NeptuneEndpointsInfoLambda() {
        this(
                EnvironmentVariableUtils.getMandatoryEnv("clusterId"),
                Integer.parseInt(EnvironmentVariableUtils.getOptionalEnv("pollingIntervalSeconds", "15")),
                EnvironmentVariableUtils.getOptionalEnv("suspended", "none"),
                Boolean.parseBoolean(EnvironmentVariableUtils.getOptionalEnv("collectCloudWatchMetrics", "false"))
        );
    }

    public NeptuneEndpointsInfoLambda(String clusterId, int pollingIntervalSeconds, String suspendedEndpoints, boolean collectCloudWatchMetrics) {

        this.refreshAgent = ClusterEndpointsRefreshAgent.usingManagementApi()
                .withClusterId(clusterId)
                .withRegion(RegionUtils.getCurrentRegionName())
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withCollectCloudWatchMetrics(collectCloudWatchMetrics)
                .build();

        this.neptuneClusterMetadata.set(refreshAgent.getClusterMetadata());
        this.suspendedEndpoints = suspendedEndpoints.toLowerCase();

        logger.info(String.format("clusterId: %s", clusterId));
        logger.info(String.format("pollingIntervalSeconds: %s", pollingIntervalSeconds));
        logger.info(String.format("suspendedEndpoints: %s", this.suspendedEndpoints));
        logger.info(String.format("collectCloudWatchMetrics: %s", collectCloudWatchMetrics));

        refreshAgent.startPollingNeptuneAPI(
                (OnNewClusterMetadata) metadata -> neptuneClusterMetadata.set(metadata),
                pollingIntervalSeconds,
                TimeUnit.SECONDS);
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

        EndpointsType endpointsType = null;

        Scanner scanner = new Scanner(input);
        if (scanner.hasNext()) {
            String param = scanner.next().replace("\"", "");
            if (!param.isEmpty()) {
                endpointsType = EndpointsType.valueOf(param);
            }
        }

        if (endpointsType != null) {
            returnEndpointListForLegacyClient(endpointsType, output);
        } else {
            returnClusterMetadata(output);
        }
    }

    private void returnClusterMetadata(OutputStream output) throws IOException {

        logger.info("Returning cluster metadata");

        NeptuneClusterMetadata clusterMetadata = addAnnotations(neptuneClusterMetadata.get());
        String results = clusterMetadata.toJsonString();

        logger.info("Results: " + results);

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(output, UTF_8))) {
            writer.write(results);
            writer.flush();
        }
    }

    private void returnEndpointListForLegacyClient(EndpointsType endpointsType,
                                                   OutputStream output) throws IOException {

        logger.info("Returning list of endpoints for EndpointsType: " + endpointsType);

        NeptuneClusterMetadata clusterMetadata = addAnnotations(neptuneClusterMetadata.get());
        EndpointCollection endpoints = endpointsType.getEndpoints(clusterMetadata);

        Collection<String> addresses = new ArrayList<>();
        for (Endpoint endpoint : endpoints) {
            addresses.add(endpoint.getAddress());
        }

        String results = String.join(",", addresses);
        logger.info("Results: " + results);

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(output, UTF_8))) {
            writer.write(results);
            writer.flush();
        }
    }

    private NeptuneClusterMetadata addAnnotations(NeptuneClusterMetadata clusterMetadata) {
        for (NeptuneInstanceMetadata instance : clusterMetadata.getInstances()) {
            if (suspendedEndpoints.equals("all")) {
                setSuspended(instance);
            } else if (suspendedEndpoints.equals("writer") && instance.isPrimary()) {
                setSuspended(instance);
            } else if (suspendedEndpoints.equals("reader") && instance.isReader()) {
                setSuspended(instance);
            }
        }
        if (suspendedEndpoints.equals("all")){
            setSuspended(clusterMetadata.getClusterEndpoint());
            setSuspended(clusterMetadata.getReaderEndpoint());
        } else if (suspendedEndpoints.equals("writer")){
            setSuspended(clusterMetadata.getClusterEndpoint());
        } else if (suspendedEndpoints.equals("reader")){
            setSuspended(clusterMetadata.getReaderEndpoint());
        }
        return clusterMetadata;
    }

    private static void setSuspended(Endpoint endpoint) {
        endpoint.setAnnotation(SuspendedEndpoints.STATE_ANNOTATION, SuspendedEndpoints.SUSPENDED);
    }

}
