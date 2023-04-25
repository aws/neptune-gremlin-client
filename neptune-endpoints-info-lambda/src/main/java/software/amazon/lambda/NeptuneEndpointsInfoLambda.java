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

    private final ClusterEndpointsRefreshAgent refreshAgent;
    private final AtomicReference<NeptuneClusterMetadata> neptuneClusterMetadata = new AtomicReference<>();
    private final String suspendedEndpoints;

    public NeptuneEndpointsInfoLambda() {
        this(
                EnvironmentVariableUtils.getMandatoryEnv("clusterId"),
                Integer.parseInt(EnvironmentVariableUtils.getOptionalEnv("pollingIntervalSeconds", "15")),
                EnvironmentVariableUtils.getOptionalEnv("suspended", "none")
        );
    }

    public NeptuneEndpointsInfoLambda(String clusterId, int pollingIntervalSeconds, String suspendedEndpoints) {

        GetEndpointsFromNeptuneManagementApi fetchStrategy =
                new GetEndpointsFromNeptuneManagementApi(
                        clusterId,
                        RegionUtils.getCurrentRegionName(),
                        new DefaultAWSCredentialsProviderChain());

        this.refreshAgent = new ClusterEndpointsRefreshAgent(fetchStrategy);
        this.neptuneClusterMetadata.set(refreshAgent.getClusterMetadata());
        this.suspendedEndpoints = suspendedEndpoints.toLowerCase();

        System.out.println(String.format("clusterId: %s", clusterId));
        System.out.println(String.format("pollingIntervalSeconds: %s", pollingIntervalSeconds));
        System.out.println(String.format("suspendedEndpoints: %s", this.suspendedEndpoints));

        refreshAgent.startPollingNeptuneAPI(
                (OnNewClusterMetadata) metadata -> neptuneClusterMetadata.set(metadata),
                pollingIntervalSeconds,
                TimeUnit.SECONDS);
    }

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

        LambdaLogger logger = context.getLogger();

        EndpointsType endpointsType = null;

        Scanner scanner = new Scanner(input);
        if (scanner.hasNext()) {
            String param = scanner.next().replace("\"", "");
            if (!param.isEmpty()) {
                endpointsType = EndpointsType.valueOf(param);
            }
        }

        if (endpointsType != null) {
            returnEndpointListForLegacyClient(endpointsType, logger, output);
        } else {
            returnClusterMetadata(logger, output);
        }
    }

    private void returnClusterMetadata(LambdaLogger logger, OutputStream output) throws IOException {

        logger.log("Returning cluster metadata");

        NeptuneClusterMetadata clusterMetadata = addAnnotations(neptuneClusterMetadata.get());
        String results = clusterMetadata.toJsonString();

        logger.log("Results: " + results);

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(output, UTF_8))) {
            writer.write(results);
            writer.flush();
        }
    }

    private void returnEndpointListForLegacyClient(EndpointsType endpointsType,
                                                   LambdaLogger logger,
                                                   OutputStream output) throws IOException {

        logger.log("Returning list of endpoints for EndpointsType: " + endpointsType);

        NeptuneClusterMetadata clusterMetadata = addAnnotations(neptuneClusterMetadata.get());
        EndpointCollection endpoints = endpointsType.getEndpoints(clusterMetadata);

        Collection<String> addresses = new ArrayList<>();
        for (Endpoint endpoint : endpoints) {
            addresses.add(endpoint.getEndpoint());
        }

        String results = String.join(",", addresses);
        logger.log("Results: " + results);

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
