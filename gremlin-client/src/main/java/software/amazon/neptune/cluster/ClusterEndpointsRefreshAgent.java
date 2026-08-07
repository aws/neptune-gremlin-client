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

import org.apache.tinkerpop.gremlin.driver.EndpointCollection;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.apache.tinkerpop.gremlin.driver.RefreshTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.utils.RegionUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ClusterEndpointsRefreshAgent implements AutoCloseable {

    public interface EndpointsSupplier {
        Map<? extends EndpointsSelector, EndpointCollection> getRefreshedEndpointsForSelectors(Map<EndpointsSelector, Collection<GremlinClient>> selectors);
    }

    public static class PollingCommand implements Runnable {

        private final Collection<RefreshTask> tasks;
        private final EndpointsSupplier endpointsSupplier;

        public PollingCommand(Collection<RefreshTask> tasks, EndpointsSupplier endpointsSupplier) {
            this.tasks = tasks;
            this.endpointsSupplier = endpointsSupplier;
        }

        @Override
        public void run() {
            try {
                Map<EndpointsSelector, Collection<GremlinClient>> clientSelectors = new HashMap<>();
                for (RefreshTask task : tasks) {
                    EndpointsSelector selector = task.selector();
                    if (!clientSelectors.containsKey(selector)){
                        clientSelectors.put(selector, new ArrayList<>());
                    }
                    clientSelectors.get(selector).add(task.client());
                }
                Map<? extends EndpointsSelector, EndpointCollection> refreshResults = endpointsSupplier.getRefreshedEndpointsForSelectors(clientSelectors);
                for (Map.Entry<? extends EndpointsSelector, EndpointCollection> entry : refreshResults.entrySet()) {
                    EndpointCollection endpoints = entry.getValue();
                    for (GremlinClient client : clientSelectors.get(entry.getKey())) {
                        logger.info("Refresh: [client: {}, endpoints: {}]", client.hashCode(), endpoints);
                        client.refreshEndpoints(endpoints);
                    }
                }
            } catch (Exception e) {
                logger.error("Error while getting cluster metadata", e);
            }
        }
    }

    public static ClusterEndpointsRefreshAgent monitor(GremlinClient client,
                                                       long delay,
                                                       TimeUnit timeUnit) {
        return monitor(Collections.singletonList(client), delay, timeUnit);
    }

    public static ClusterEndpointsRefreshAgent monitor(Collection<GremlinClient> clients,
                                                       long delay,
                                                       TimeUnit timeUnit) {
        EndpointsSelector nullSelector = clusterMetadata -> {
            throw new UnsupportedOperationException();
        };
        ClusterEndpointsRefreshAgent refreshAgent = new ClusterEndpointsRefreshAgent(new GetCurrentEndpointsFromGremlinClient());

        refreshAgent.startPollingNeptuneAPI(clients.stream().map(c -> new RefreshTask(c, nullSelector)).collect(Collectors.toList()), delay, timeUnit);

        return refreshAgent;
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName) {
        return lambdaProxy(lambdaName, RegionUtils.getCurrentRegionName());
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region) {
        return lambdaProxy(lambdaName, region, IamAuthConfig.DEFAULT_PROFILE);
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, AwsCredentialsProvider credentialsProvider) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, credentialsProvider));
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, AwsCredentialsProvider credentialsProvider, ClientOverrideConfiguration clientConfiguration) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, credentialsProvider, clientConfiguration));
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, AwsCredentialsProvider credentialsProvider, ClientOverrideConfiguration clientConfiguration, SdkHttpClient.Builder<?> httpClientBuilder) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, credentialsProvider, clientConfiguration, httpClientBuilder));
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, String iamProfile) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, iamProfile));
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, String iamProfile, ClientOverrideConfiguration clientConfiguration, SdkHttpClient.Builder<?> httpClientBuilder) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, iamProfile, clientConfiguration, httpClientBuilder));
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, String iamProfile, ClientOverrideConfiguration clientConfiguration) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, iamProfile, clientConfiguration));
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId) {
        return managementApi(clusterId, RegionUtils.getCurrentRegionName());
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region) {
        return managementApi(clusterId, region, IamAuthConfig.DEFAULT_PROFILE);
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region, AwsCredentialsProvider credentialsProvider) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromNeptuneManagementApi(clusterId, region, credentialsProvider));
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region, AwsCredentialsProvider credentialsProvider, ClientOverrideConfiguration clientConfiguration) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromNeptuneManagementApi(clusterId, region, credentialsProvider, clientConfiguration));
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region, AwsCredentialsProvider credentialsProvider, ClientOverrideConfiguration clientConfiguration, SdkHttpClient.Builder<?> httpClientBuilder) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromNeptuneManagementApi(clusterId, region, credentialsProvider, clientConfiguration, httpClientBuilder));
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region, String iamProfile) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromNeptuneManagementApi(clusterId, region, iamProfile));
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region, String iamProfile, ClientOverrideConfiguration clientConfiguration) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromNeptuneManagementApi(clusterId, region, iamProfile, clientConfiguration));
    }

    private static final Logger logger = LoggerFactory.getLogger(ClusterEndpointsRefreshAgent.class);

    private static final long DEFAULT_TERMINATION_TIMEOUT_MILLIS = 5000;

    private final ClusterEndpointsFetchStrategy endpointsFetchStrategy;
    private final Object executorServiceLock = new Object();

    private volatile ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public ClusterEndpointsRefreshAgent(ClusterEndpointsFetchStrategy endpointsFetchStrategy) {
        this.endpointsFetchStrategy = endpointsFetchStrategy;
    }

    public <T extends EndpointsSelector> void startPollingNeptuneAPI(GremlinClient client,
                                                                     T selector,
                                                                     long delay,
                                                                     TimeUnit timeUnit) {

        startPollingNeptuneAPI(RefreshTask.refresh(client, selector), delay, timeUnit);
    }

    public <T extends EndpointsSelector> void startPollingNeptuneAPI(RefreshTask refreshTask,
                                                                     long delay,
                                                                     TimeUnit timeUnit) {

        startPollingNeptuneAPI(Collections.singletonList(refreshTask), delay, timeUnit);
    }

    public <T extends EndpointsSelector> void startPollingNeptuneAPI(Collection<RefreshTask> tasks,
                                                                     long delay,
                                                                     TimeUnit timeUnit) {

        schedule(new PollingCommand(tasks, this::refreshEndpoints), delay, timeUnit);
    }

    public void startPollingNeptuneAPI(OnNewClusterMetadata onNewClusterMetadata,
                                       long delay,
                                       TimeUnit timeUnit) {

        schedule(() -> {
            try {
                NeptuneClusterMetadata clusterMetadata = refreshClusterMetadata();
                logger.info("New cluster metadata: {}", clusterMetadata);
                onNewClusterMetadata.apply(clusterMetadata);
            } catch (Exception e) {
                logger.error("Error while refreshing cluster metadata", e);
            }

        }, delay, timeUnit);
    }

    /**
     * Stops polling, waiting up to 5 seconds for the in-flight polling task to terminate.
     * Use {@link #stop(long, TimeUnit)} to supply a different timeout or to observe whether the
     * task actually terminated.
     */
    public void stop() {
        stop(DEFAULT_TERMINATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * Stops polling and waits for the in-flight polling task to terminate.
     * <p>
     * Waiting for termination ensures a subsequent call to {@code startPollingNeptuneAPI} cannot run a
     * new polling task alongside the old one. If the wait does not complete, the agent remains marked
     * as running and a restart throws {@link IllegalStateException} rather than adding a second poller;
     * call {@code stop} again to carry on waiting for the same task.
     * <p>
     * Pass a timeout of zero to return without waiting. The agent is then immediately restartable, at
     * the risk of the old and new polling tasks overlapping.
     *
     * @param timeout  the maximum time to wait for the polling task to terminate
     * @param timeUnit the unit of the {@code timeout} argument
     * @return {@code true} if the polling task terminated, {@code false} if the wait timed out or was
     * interrupted, or if {@code timeout} was zero and no wait was attempted
     */
    public boolean stop(long timeout, TimeUnit timeUnit) {

        synchronized (executorServiceLock) {

            ScheduledExecutorService executorService = scheduledExecutorService;
            executorService.shutdownNow();

            if (timeout <= 0) {
                isRunning.set(false);
                return false;
            }

            try {
                if (executorService.awaitTermination(timeout, timeUnit)) {
                    isRunning.set(false);
                    return true;
                }
                logger.warn("Timed out waiting for the polling task to terminate. " +
                        "The refresh agent cannot be restarted until it has terminated.");
            } catch (InterruptedException e) {
                logger.warn("Interrupted while waiting for the polling task to terminate");
                Thread.currentThread().interrupt();
            }

            return false;
        }
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    public <T extends EndpointsSelector> EndpointCollection getEndpoints(T selector) {
        return endpointsFetchStrategy.getEndpoints(Collections.singletonList(selector), false).get(selector);
    }

    public NeptuneClusterMetadata getClusterMetadata() {
        return endpointsFetchStrategy.clusterMetadataSupplier().getClusterMetadata();
    }

    /**
     * Runs a no-op on the polling thread and waits for it to complete, forcing any refresh that is
     * already queued or in flight to finish. Use this to ensure refreshes occur in a timely manner in an
     * environment whose execution can be suspended between invocations, such as an AWS Lambda function.
     *
     * @throws IllegalStateException if the agent has been stopped. Waking a stopped agent cannot resume
     *                               polling, so recreating its executor here would start a thread that
     *                               never polls and that outlives {@link #stop()} and {@link #close()}.
     */
    public void awake() throws InterruptedException, ExecutionException {
        Future<?> future;

        synchronized (executorServiceLock) {
            if (scheduledExecutorService.isShutdown()) {
                throw new IllegalStateException(
                        "Refresh agent has been stopped. Call startPollingNeptuneAPI to resume polling.");
            }
            future = scheduledExecutorService.submit(() -> {
            });
        }

        future.get();
    }

    /**
     * Marks the agent as running and schedules the polling command, recreating the executor service if it
     * was shut down by a previous call to {@link #stop()}.
     * <p>
     * The whole transition happens under {@link #executorServiceLock} so that a concurrent start or stop
     * cannot interleave: {@code isRunning} is only set once the command is scheduled, and so always
     * reflects whether a polling task is live.
     *
     * @throws IllegalStateException if the agent is already running
     */
    private void schedule(Runnable command, long delay, TimeUnit timeUnit) {
        synchronized (executorServiceLock) {

            if (isRunning.get()) {
                throw new IllegalStateException("Refresh agent is already running");
            }

            if (scheduledExecutorService.isShutdown()) {
                scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            }

            scheduledExecutorService.scheduleWithFixedDelay(command, delay, delay, timeUnit);

            isRunning.set(true);
        }
    }

    private Map<? extends EndpointsSelector, EndpointCollection> refreshEndpoints(Map<EndpointsSelector, Collection<GremlinClient>> clientSelectors) {
        return endpointsFetchStrategy.getEndpoints(clientSelectors, true);
    }

    private NeptuneClusterMetadata refreshClusterMetadata() {
        return endpointsFetchStrategy.clusterMetadataSupplier().refreshClusterMetadata();
    }
}
