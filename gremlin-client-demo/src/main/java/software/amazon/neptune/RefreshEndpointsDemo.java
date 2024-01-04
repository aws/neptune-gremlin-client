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
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.DatabaseEndpoint;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.apache.tinkerpop.gremlin.driver.GremlinCluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.cluster.ClusterEndpointsRefreshAgent;
import software.amazon.neptune.cluster.NeptuneGremlinClusterBuilder;
import software.amazon.utils.RegionUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Command(name = "refresh-endpoints-demo", description = "Demo using refreshEndpoints method to supply list of endpoints")
public class RefreshEndpointsDemo implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RefreshAgentDemo.class);

    @Option(name = {"--endpoint"}, description = "Amazon Neptune instance endpoint.")
    private List<String> endpoints;

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

    @Option(name = {"--refresh-count"}, description = "Number of queries between calls to refreshEndpoints()")
    @Once
    private int refreshCount = 100;

    private final AtomicInteger index = new AtomicInteger(0);

    @Override
    public void run() {

        try {

            String endpoint = endpoints.get(index.getAndIncrement() % endpoints.size() );

            NeptuneGremlinClusterBuilder builder = NeptuneGremlinClusterBuilder.build()
                    .enableSsl(!disableSsl)
                    .enableIamAuth(enableIam)
                    .iamProfile(profile)
                    .addContactPoints(endpoint)
                    .minConnectionPoolSize(3)
                    .maxConnectionPoolSize(3)
                    .port(neptunePort);

            if (StringUtils.isNotEmpty(serviceRegion)) {
                builder = builder.serviceRegion(serviceRegion);
            }

            GremlinCluster cluster = builder.create();

            GremlinClient client = cluster.connect();

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
                    if (i % refreshCount == 0){
                        System.out.println();
                        endpoint = endpoints.get(index.getAndIncrement() % endpoints.size() );
                        System.out.println("Refreshing endpoint with " + endpoint);
                        client.refreshEndpoints(new DatabaseEndpoint().withAddress(endpoint));
                    }
                } catch (Exception e) {
                    logger.warn("Error processing query: {}", e.getMessage());
                }
            }

            client.close();
            cluster.close();

        } catch (Exception e) {
            System.err.println("An error occurred while connecting to Neptune:");
            e.printStackTrace();
            System.exit(-1);
        }
    }


}
