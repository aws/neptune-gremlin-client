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

package software.amazon.neptune;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.PortType;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.EndpointCollection;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.apache.tinkerpop.gremlin.driver.GremlinCluster;
import org.apache.tinkerpop.gremlin.driver.RefreshTask;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.cluster.*;
import software.amazon.utils.RegionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Command(name = "custom-selectors-demo", description = "Demo using custom endpoint selectors")
public class CustomSelectorsDemo implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CustomSelectorsDemo.class);

    @Option(name = {"--cluster-id"}, description = "Amazon Neptune cluster Id. You must supply either a cluster Id or the name of an AWS Lambda proxy function (using --lambda-proxy).")
    @Once
    @RequireOnlyOne(tag = "cluster-id-or-lambda-proxy")
    private String clusterId;

    @Option(name = {"--lambda-proxy"}, description = "Name of the AWS Lambda proxy function used to fetch cluster topology. You must supply either the name of an AWS Lambda proxy function or a cluster Id or (using --cluster-id). If you are using a Lambda proxy, amke sure you have installed it (using the AWS CloudFormation template) before running this demo.")
    @Once
    @RequireOnlyOne(tag = "cluster-id-or-lambda-proxy")
    private String lambdaProxy;

    @Option(name = {"--port"}, description = "Neptune port (optional, default 8182)")
    @Port(acceptablePorts = {PortType.SYSTEM, PortType.USER})
    @Once
    private int neptunePort = 8182;

    @Option(name = {"--disable-ssl"}, description = "Disables connectivity over SSL (optional, default false)")
    @Once
    private boolean disableSsl = false;

    @Option(name = {"--enable-iam"}, description = "Enables IAM database authentication (optional, default false)")
    @Once
    private boolean enableIam = false;

    @Option(name = {"--query-count"}, description = "Number of queries to execute")
    @Once
    private int queryCount = 1000000;

    @Option(name = {"--log-level"}, description = "Log level")
    @Once
    private String logLevel = "info";

    @Option(name = {"--profile"}, description = "Credentials profile")
    @Once
    private String profile = "default";

    @Option(name = {"--service-region"}, description = "Neptune service region")
    @Once
    private String serviceRegion = null;

    @Option(name = {"--interval"}, description = "Interval (in seconds) between refreshing addresses")
    @Once
    private int intervalSeconds = 15;

    @Override
    public void run() {

        try {

            EndpointsSelector writerSelector = (cluster) -> {
                List<NeptuneInstanceMetadata> endpoints = cluster.getInstances().stream()
                        .filter(NeptuneInstanceMetadata::isPrimary)
                        .filter(NeptuneInstanceMetadata::isAvailable)
                        .collect(Collectors.toList());
                return endpoints.isEmpty() ?
                        new EndpointCollection(Collections.singletonList(cluster.getClusterEndpoint())) :
                        new EndpointCollection(endpoints);
            };


            EndpointsSelector readerSelector = (cluster) ->
                    new EndpointCollection(
                            cluster.getInstances().stream()
                                    .filter(NeptuneInstanceMetadata::isReader)
                                    .filter(NeptuneInstanceMetadata::isAvailable)
                                    .collect(Collectors.toList()));

            ClusterEndpointsRefreshAgent refreshAgent = createRefreshAgent();

            GremlinCluster writerCluster = createCluster(writerSelector, refreshAgent);
            GremlinCluster readerCluster = createCluster(readerSelector, refreshAgent);

            GremlinClient writer = writerCluster.connect();
            GremlinClient reader = readerCluster.connect();

            refreshAgent.startPollingNeptuneAPI(
                    Arrays.asList(
                            RefreshTask.refresh(writer, writerSelector),
                            RefreshTask.refresh(reader, readerSelector)
                    ),
                    60,
                    TimeUnit.SECONDS);

            GraphTraversalSource gWriter = createGraphTraversalSource(writer);
            GraphTraversalSource gReader = createGraphTraversalSource(reader);

            for (int i = 0; i < queryCount; i++) {
                try {
                    if (i % 2 == 1) {
                        List<Map<Object, Object>> results = gReader.V().limit(10).valueMap(true).toList();
                        for (Map<Object, Object> result : results) {
                            //Do nothing
                        }
                    } else {
                        gWriter.addV("TestNode").property("my-id", i).next();
                    }

                } catch (Exception e) {
                    logger.warn("Error processing query: {}", e.getMessage());
                }

                if (i % 10000 == 0) {
                    System.out.println();
                    System.out.println("Number of queries: " + i);
                }
            }

            refreshAgent.close();

            writer.close();
            reader.close();

            writerCluster.close();
            readerCluster.close();

        } catch (Exception e) {
            System.err.println("An error occurred while connecting to Neptune:");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static GraphTraversalSource createGraphTraversalSource(GremlinClient writer) {
        DriverRemoteConnection connection = DriverRemoteConnection.using(writer);
        return AnonymousTraversalSource.traversal().withRemote(connection);
    }

    private ClusterEndpointsRefreshAgent createRefreshAgent() {

        if (StringUtils.isNotEmpty(clusterId)) {
            return ClusterEndpointsRefreshAgent.managementApi(clusterId, RegionUtils.getCurrentRegionName(), profile);
        } else if (StringUtils.isNotEmpty(lambdaProxy)) {
            return ClusterEndpointsRefreshAgent.lambdaProxy(lambdaProxy, RegionUtils.getCurrentRegionName(), profile);
        } else {
            throw new IllegalStateException("You must supply either a cluster Id or AWS Lambda proxy name");
        }

    }

    private GremlinCluster createCluster(EndpointsSelector selector, ClusterEndpointsRefreshAgent refreshAgent) {

        NeptuneGremlinClusterBuilder clusterBuilder = NeptuneGremlinClusterBuilder.build()
                .enableSsl(!disableSsl)
                .enableIamAuth(enableIam)
                .iamProfile(profile)
                .addContactPoints(refreshAgent.getEndpoints(selector))
                .minConnectionPoolSize(3)
                .maxConnectionPoolSize(3)
                .port(neptunePort);

        if (StringUtils.isNotEmpty(serviceRegion)) {
            clusterBuilder = clusterBuilder.serviceRegion(serviceRegion);
        }

        return clusterBuilder.create();
    }
}
