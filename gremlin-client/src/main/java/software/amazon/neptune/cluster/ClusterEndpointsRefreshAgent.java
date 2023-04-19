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
import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.RegionUtils;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClusterEndpointsRefreshAgent implements AutoCloseable {

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName) {
        return lambdaProxy(lambdaName, RegionUtils.getCurrentRegionName());
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region) {
        return lambdaProxy(lambdaName, region, IamAuthConfig.DEFAULT_PROFILE);
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, AWSCredentialsProvider credentialsProvider) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, credentialsProvider));
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, credentialsProvider, clientConfiguration));
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, String iamProfile) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, iamProfile));
    }

    public static ClusterEndpointsRefreshAgent lambdaProxy(String lambdaName, String region, String iamProfile, ClientConfiguration clientConfiguration) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromLambdaProxy(lambdaName, region, iamProfile, clientConfiguration));
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId) {
        return managementApi(clusterId, RegionUtils.getCurrentRegionName());
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region) {
        return managementApi(clusterId, region, IamAuthConfig.DEFAULT_PROFILE);
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region, AWSCredentialsProvider credentialsProvider) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromNeptuneManagementApi(clusterId, region, credentialsProvider));
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region, AWSCredentialsProvider credentialsProvider, ClientConfiguration clientConfiguration) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromNeptuneManagementApi(clusterId, region, credentialsProvider, clientConfiguration));
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region, String iamProfile) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromNeptuneManagementApi(clusterId, region, iamProfile));
    }

    public static ClusterEndpointsRefreshAgent managementApi(String clusterId, String region, String iamProfile, ClientConfiguration clientConfiguration ) {
        return new ClusterEndpointsRefreshAgent(
                new GetEndpointsFromNeptuneManagementApi(clusterId, region, iamProfile, clientConfiguration));
    }

    private static final Logger logger = LoggerFactory.getLogger(ClusterEndpointsRefreshAgent.class);

    private final ClusterEndpointsFetchStrategy endpointsFetchStrategy;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

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

        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                Map<EndpointsSelector, GremlinClient> m = new HashMap<>();
                for (RefreshTask task : tasks) {
                    m.put(task.selector(), task.client());
                }
                Map<? extends EndpointsSelector, EndpointCollection> refreshResults = refreshEndpoints(m.keySet());
                for (Map.Entry<? extends EndpointsSelector, EndpointCollection> entry : refreshResults.entrySet()) {
                    EndpointCollection endpoints = entry.getValue();
                    GremlinClient client = m.get(entry.getKey());
                    logger.info("Refresh: [client: {}, endpoints: {}]", client.hashCode(), endpoints);
                    client.refreshEndpoints(endpoints);
                }
            } catch (Exception e) {
                logger.error("Error while getting cluster metadata", e);
            }

        }, delay, delay, timeUnit);
    }

    public void startPollingNeptuneAPI(OnNewClusterMetadata onNewClusterMetadata,
                                       long delay,
                                       TimeUnit timeUnit) {

        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            try {
                NeptuneClusterMetadata clusterMetadata = refreshClusterMetadata();
                logger.info("New cluster metadata: {}", clusterMetadata);
                onNewClusterMetadata.apply(clusterMetadata);
            } catch (Exception e) {
                logger.error("Error while refreshing cluster metadata", e);
            }

        }, delay, delay, timeUnit);
    }

    public void stop() {
        scheduledExecutorService.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    public Map<? extends EndpointsSelector, EndpointCollection> refreshEndpoints(Collection<EndpointsSelector> selectors) {
        return endpointsFetchStrategy.getEndpoints(selectors, true);
    }

    public EndpointCollection refreshEndpoints(EndpointsSelector selector) {
        return endpointsFetchStrategy.getEndpoints(Arrays.asList(selector), true).get(selector);
    }

    public <T extends EndpointsSelector> EndpointCollection getEndpoints(T selector) {
        return endpointsFetchStrategy.getEndpoints(Arrays.asList(selector), false).get(selector);
    }

    public NeptuneClusterMetadata getClusterMetadata() {
        return endpointsFetchStrategy.clusterMetadataSupplier().getClusterMetadata();
    }

    public NeptuneClusterMetadata refreshClusterMetadata() {
        return endpointsFetchStrategy.clusterMetadataSupplier().refreshClusterMetadata();
    }
}
