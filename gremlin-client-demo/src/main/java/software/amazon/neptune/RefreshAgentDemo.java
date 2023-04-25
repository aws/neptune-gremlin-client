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
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.apache.tinkerpop.gremlin.driver.GremlinCluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.cluster.ClusterEndpointsRefreshAgent;
import software.amazon.neptune.cluster.EndpointsType;
import software.amazon.neptune.cluster.GetEndpointsFromNeptuneManagementApi;
import software.amazon.neptune.cluster.NeptuneGremlinClusterBuilder;
import software.amazon.utils.RegionUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Command(name = "refresh-agent-demo", description = "Demo using refresh client with topology aware cluster and client")
public class RefreshAgentDemo implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RefreshAgentDemo.class);

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

    @Option(name = {"--enable-ssl"}, description = "Enables connectivity over SSL (optional, default true)")
    @Once
    private boolean enableSsl = true;

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

            EndpointsType selector = EndpointsType.ReadReplicas;

            ClusterEndpointsRefreshAgent refreshAgent = createRefreshAgent();

            NeptuneGremlinClusterBuilder builder = NeptuneGremlinClusterBuilder.build()
                    .enableSsl(enableSsl)
                    .enableIamAuth(enableIam)
                    .iamProfile(profile)
                    .addContactPoints(refreshAgent.getEndpoints(selector))
                    .minConnectionPoolSize(3)
                    .maxConnectionPoolSize(3)
                    .port(neptunePort);

            if (StringUtils.isNotEmpty(serviceRegion)) {
                builder = builder.serviceRegion(serviceRegion);
            }

            GremlinCluster cluster = builder.create();

            GremlinClient client = cluster.connect();

            refreshAgent.startPollingNeptuneAPI(
                    client,
                    selector,
                    intervalSeconds,
                    TimeUnit.SECONDS
            );

            DriverRemoteConnection connection = DriverRemoteConnection.using(client);
            GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(connection);

            for (int i = 0; i < queryCount; i++) {
                try {
                    List<Map<Object, Object>> results = g.V().limit(10).valueMap(true).toList();
                    for (Map<Object, Object> result : results) {
                        //Do nothing
                    }
                    if (i % 10000 == 0) {
                        System.out.println();
                        System.out.println("Number of queries: " + i);
                    }
                } catch (Exception e) {
                    logger.warn("Error processing query: {}", e.getMessage());
                }
            }

            refreshAgent.close();
            client.close();
            cluster.close();

        } catch (Exception e) {
            System.err.println("An error occurred while connecting to Neptune:");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private ClusterEndpointsRefreshAgent createRefreshAgent() {

        if (StringUtils.isNotEmpty(clusterId)) {
            GetEndpointsFromNeptuneManagementApi fetchStrategy = new GetEndpointsFromNeptuneManagementApi(
                    clusterId,
                    RegionUtils.getCurrentRegionName(),
                    profile
            );

            return new ClusterEndpointsRefreshAgent(fetchStrategy);
        } else if (StringUtils.isNotEmpty(lambdaProxy)) {
            return ClusterEndpointsRefreshAgent.lambdaProxy(lambdaProxy, RegionUtils.getCurrentRegionName(), profile);
        } else {
            throw new IllegalStateException("You must supply either a cluster Id or AWS Lambda proxy name");
        }

    }
}