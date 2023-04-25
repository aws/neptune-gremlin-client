package software.amazon.neptune;

import com.evanlennick.retry4j.CallExecutor;
import com.evanlennick.retry4j.CallExecutorBuilder;
import com.evanlennick.retry4j.Status;
import com.evanlennick.retry4j.config.RetryConfig;
import com.evanlennick.retry4j.config.RetryConfigBuilder;
import com.evanlennick.retry4j.exception.RetriesExhaustedException;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Port;
import com.github.rvesse.airline.annotations.restrictions.PortType;
import com.github.rvesse.airline.annotations.restrictions.RequireOnlyOne;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.ClusterContext;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.apache.tinkerpop.gremlin.driver.GremlinCluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.neptune.cluster.*;
import software.amazon.utils.RegionUtils;
import software.amazon.utils.RetryUtils;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Command(name = "retry-demo", description = "Demonstrates backoff-and-retry strategies when creating connecting and submitting query")
public class RetryDemo implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RetryDemo.class);

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

            ClusterEndpointsRefreshAgent refreshAgent = createRefreshAgent();

            RetryConfig retryConfig = new RetryConfigBuilder()
                    .retryOnCustomExceptionLogic(new Function<Exception, Boolean>() {
                        @Override
                        public Boolean apply(Exception e) {
                            RetryUtils.Result result = RetryUtils.isRetryableException(e);
                            logger.info("isRetriableException: {}", result);
                            return result.isRetryable();
                        }
                    })
                    .withExponentialBackoff()
                    .withMaxNumberOfTries(5)
                    .withDelayBetweenTries(1, ChronoUnit.SECONDS)
                    .build();

            ClusterContext readerContext = createClusterContext(retryConfig, refreshAgent, EndpointsType.ReadReplicas);
            ClusterContext writerContext = createClusterContext(retryConfig, refreshAgent, EndpointsType.Primary);

            // Use same GraphTraversalSources across threads
            GraphTraversalSource gReader = readerContext.graphTraversalSource();
            GraphTraversalSource gWriter = writerContext.graphTraversalSource();

            logger.info("Starting queries...");

            AtomicInteger currentQueryCount = new AtomicInteger(0);

            ExecutorService taskExecutor = Executors.newFixedThreadPool(5);

            for (int i = 0; i < 5; i++) {
                taskExecutor.submit(new Runnable() {
                    @Override
                    public void run() {

                        try {
                            int count = 0;
                            int readCount = 0;
                            int writeCount = 0;
                            int tries = 0;
                            int failedReads = 0;
                            int failedWrites = 0;

                            CallExecutor executor = new CallExecutorBuilder()
                                    .config(retryConfig)
                                    .build();

                            while (count < queryCount) {

                                count = currentQueryCount.incrementAndGet();

                                if (count % 7 == 0) {
                                    // write
                                    writeCount++;

                                    Callable<Edge> query = () ->
                                            gWriter.addV("Thing").as("v1").
                                                    addV("Thing").as("v2").
                                                    addE("Connection").from("v1").to("v2").
                                                    next();

                                    try {
                                        Status<Edge> status = executor.execute(query);
                                        tries += status.getTotalTries();
                                    } catch (RetriesExhaustedException e) {
                                        failedWrites++;
                                    }


                                } else {
                                    // read
                                    readCount++;

                                    Callable<List<Map<Object, Object>>> query = () ->
                                            gReader.V().limit(10).valueMap(true).toList();


                                    try {
                                        Status<List<Map<Object, Object>>> status = executor.execute(query);
                                        tries += status.getTotalTries();

                                        List<Map<Object, Object>> results = status.getResult();

                                        for (Map<Object, Object> result : results) {
                                            //Do nothing
                                        }
                                    } catch (RetriesExhaustedException e) {
                                        failedReads++;
                                    }
                                }
                                logger.info("Progress: [queries: {}, tries: {}, reads: {}, writes: {}, failedReads: {}, failedWrites: {}]",
                                        (readCount + writeCount),
                                        tries,
                                        readCount,
                                        writeCount,
                                        failedReads,
                                        failedWrites);
                            }
                        } catch (Exception e) {
                            logger.error("Unexpected error", e);
                        }
                    }
                });
            }


            taskExecutor.shutdown();

            try {
                if (!taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
                    logger.warn("Timeout expired with uncompleted tasks");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }

            logger.info("Closing...");

            refreshAgent.close();
            readerContext.close();
            writerContext.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private ClusterContext createClusterContext(RetryConfig retryConfig,
                                                ClusterEndpointsRefreshAgent refreshAgent,
                                                EndpointsSelector selector) {


        logger.info("Creating ClusterContext for {}", selector);

        CallExecutor executor = new CallExecutorBuilder()
                .config(retryConfig)
                .build();


        Status<ClusterContext> status = executor.execute((Callable<ClusterContext>) () -> {

            NeptuneGremlinClusterBuilder builder = NeptuneGremlinClusterBuilder.build()
                    .enableSsl(enableSsl)
                    .enableIamAuth(enableIam)
                    .iamProfile(profile)
                    .addContactPoints(refreshAgent.getEndpoints(selector))
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

            return new ClusterContext(cluster, client, g);
        });

        return status.getResult();
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
