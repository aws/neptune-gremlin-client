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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.PortType;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;
import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.commons.lang3.StringUtils;

import software.amazon.neptune.cluster.ClusterEndpointsRefreshAgent;
import software.amazon.neptune.cluster.EndpointsType;
import software.amazon.neptune.cluster.NeptuneGremlinClusterBuilder;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


@Command(name = "continuous-read-demo", description = "Continuous read demo using the Neptune Gremlin Client")
public class ContinuousReadDemo implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ContinuousReadDemo.class);

    @Option(name = {"--cluster-id"}, description = "Amazon Neptune cluster Id")
    @Once
    private String clusterId;

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

    @Option(name = {"--profile"}, description = "Credentials profile")
    @Once
    private String profile = "default";

    @Option(name = {"--service-region"}, description = "Neptune service region (deafult us-west-2)")
    @Once
    private String serviceRegion = "us-west-2";

    @Option(name = {"--duration"}, description = "Duration in seconds for continuous reading (default 60)")
    @Once
    private int durationSeconds = 60;

    @Option(name = {"--min-connections"}, description = "Minimum connection pool size should be >=5 (default 5)")
    @Once
    private int minConnectionPoolSize = 5;

    @Option(name = {"--vertex-count"}, description = "Number of test vertices to create (default 100)")
    @Once
    private int vertexCount = 100;

    @Option(name = {"--threads"}, description = "Number of reader threads (default 20)")
    @Once
    private int numReaderThreads = 20;


    @Override
    public void run() {

        if (StringUtils.isBlank(clusterId)) {
            throw new IllegalArgumentException("You must provide --cluster-id");
        }

        minConnectionPoolSize = Math.max(5, minConnectionPoolSize); // Ensure never less than 5
        final int maxConnectionPoolSize = minConnectionPoolSize * 5; // Always 5x of min

        logger.info("Starting continuous read demo for {} seconds...", durationSeconds);
        logger.info("Connection pool config - Min: {}, Max: {}", minConnectionPoolSize, maxConnectionPoolSize);

//        ChooseEndpointStrategy strategy = new EqualConcurrentUsageEndpointStrategy();
//        ChooseEndpointStrategy strategy = new RoundRobinEndpointStrategy();
//        ChooseEndpointStrategy strategy = new ConcurrentUsageEndpointStrategy();
        final EndpointFilter endpointFilter = new StatusEndpointFilter(serviceRegion);

        GremlinCluster writerCluster = null;
        GremlinClient writerClient = null;
        GremlinCluster readerCluster = null;
        GremlinClient readerClient = null;
        ClusterEndpointsRefreshAgent writerAgent = null;
        ClusterEndpointsRefreshAgent readerAgent = null;
        DriverRemoteConnection writerConnection = null;
        DriverRemoteConnection readerConnection = null;

        try {
            // Setup writer refresh agent and connection with health check
            logger.info("Setting up writer cluster with health check refresh agent...");
            writerAgent = ClusterEndpointsRefreshAgent.managementApi(clusterId, serviceRegion, profile);

            writerCluster = NeptuneGremlinClusterBuilder.build()
                    .enableSsl(!disableSsl)
                    .enableIamAuth(enableIam)
                    .iamProfile(profile)
                    .serviceRegion(serviceRegion)
                    .addContactPoints(writerAgent.getEndpoints(EndpointsType.Primary))
                    .port(neptunePort)
                    .minConnectionPoolSize(minConnectionPoolSize)
                    .maxConnectionPoolSize(maxConnectionPoolSize)
                    .create();

            writerClient = writerCluster.connect();

            // Start refresh agent for writer
            writerAgent.startPollingNeptuneAPI(
                    writerClient,
                    EndpointsType.Primary,
                    30,
                    TimeUnit.SECONDS
            );

            logger.info("Writer cluster connection established with health checks.");
            // Create GraphTraversalSource for writer
            writerConnection = DriverRemoteConnection.using(writerClient);
            GraphTraversalSource gWriter = AnonymousTraversalSource.traversal().withRemote(writerConnection);
            // Clear existing data and write vertices
            logger.info("Preparing test data...");
            gWriter.V().drop().iterate();

            List<Object> vertexIds = new ArrayList<>();
            for (int i = 1; i <= vertexCount; i++) {
                Object vertexId = gWriter.addV("TestVertex")
                        .property("name", "vertex_" + i)
                        .property("index", i)
                        .next()
                        .id();
                vertexIds.add(vertexId);
            }

            logger.info("Created {} vertices for testing.", vertexCount);
            Thread.sleep(1000); // Wait for replication
            try {
                writerCluster.close();
                writerAgent.close();
            } catch (Exception e) {
                // Don't do anything.
                logger.error("Failed to close writer connection", e);
            }

            // Setup reader refresh agent and connection with health check
            logger.info("Setting up reader cluster with health check refresh agent...");
            readerAgent = ClusterEndpointsRefreshAgent.managementApi(clusterId, serviceRegion, profile);

            readerCluster = NeptuneGremlinClusterBuilder.build()
                    .enableSsl(!disableSsl)
                    .enableIamAuth(enableIam)
                    .iamProfile(profile)
                    .serviceRegion(serviceRegion)
                    .addContactPoints(readerAgent.getEndpoints(EndpointsType.ReadReplicas))
                    .port(neptunePort)
                    .minConnectionPoolSize(minConnectionPoolSize)
                    .maxConnectionPoolSize(maxConnectionPoolSize)
                    .maxWaitForConnection(1000)
//                    .keepAliveInterval(10_000) // Ping every 10 seconds.
//                    .chooseConnectionStrategySupplier(() -> strategy)
                    .endpointFilter(endpointFilter)
                    .create();

            readerClient = readerCluster.connect();

            // Start refresh agent for reader
            readerAgent.startPollingNeptuneAPI(
                    readerClient,
                    EndpointsType.ReadReplicas,
                    30,
                    TimeUnit.SECONDS
            );

            logger.info("Reader cluster connection established with health checks.");


            // Create GraphTraversalSource for reader
            readerConnection = DriverRemoteConnection.using(readerClient);
            GraphTraversalSource gReader = AnonymousTraversalSource.traversal().withRemote(readerConnection);


            // Continuous reading for specified duration
            logger.info("Starting continuous read operations...");
            final long startTime = System.currentTimeMillis();
            final long endTime = startTime + (durationSeconds * 1000L);
            AtomicLong readCount = new AtomicLong(0);
            Random random = new Random();
            ExecutorService executorService = Executors.newFixedThreadPool(numReaderThreads);
            AtomicLong lastElapsed = new AtomicLong(0);
            final Runnable runnable = () -> {

                while (System.currentTimeMillis() < endTime) {
                    UUID uuid = UUID.randomUUID();
                    try {
                        // Read a random vertex
                        logger.info("STARTING_REQUEST: {}", uuid);

                        Object randomVertexId = vertexIds.get(random.nextInt(vertexIds.size()));
                        Map<Object, Object> vertex = gReader.V(randomVertexId).valueMap(true).next();
                        readCount.incrementAndGet();
                        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
                        if (readCount.get() % 100 == 0 || (elapsed - lastElapsed.get()) > 1) {
                            logger.info("Read {} vertices (elapsed: {}s)", readCount, elapsed);
                            lastElapsed.set(elapsed);
                        }

                        logger.info("ENDING_REQUEST: {}", uuid);
                    } catch (Exception e) {
                        logger.error("REQUEST_ID {}, Read error",uuid, e);
                        try {
                            Thread.sleep(10); // Wait longer on error
                        } catch (InterruptedException ex) {
                            // NO OP
                        }
                    }
                }
            };
            for (int i = 0; i < numReaderThreads; i++) {
                executorService.execute(runnable);
            }
            executorService.shutdown();
            executorService.awaitTermination(durationSeconds, TimeUnit.SECONDS);

            long totalElapsed = (System.currentTimeMillis() - startTime) / 1000;
            logger.info("Completed continuous reading: {} vertices in {} seconds", readCount, totalElapsed);

        } catch (Exception e) {
            System.err.println("Error during Neptune Countinuos Read Demo: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            logger.info("Demo completed - exiting immediately");
            System.exit(0);
        }
    }
}