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
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterEndpointsRefreshAgentRestartTest {

    private static final long AWAIT_TIMEOUT_SECONDS = 10;
    private static final long POLLING_DELAY_MILLIS = 50;

    /**
     * Stops an agent that is midway through polling, then restarts it and asserts that polling resumes.
     */
    private interface StopAction {
        void stop(ClusterEndpointsRefreshAgent agent);
    }

    @Test
    public void shouldResumePollingAfterStopAndRestart() throws InterruptedException {
        assertResumesPollingAfterRestart(ClusterEndpointsRefreshAgent::stop);
    }

    @Test
    public void shouldResumePollingWhenStoppedWithAnExplicitTimeout() throws InterruptedException {
        assertResumesPollingAfterRestart(agent -> agent.stop(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void shouldResumePollingWhenStoppedWithoutWaiting() throws InterruptedException {
        assertResumesPollingAfterRestart(agent -> agent.stop(0, TimeUnit.MILLISECONDS));
    }

    private void assertResumesPollingAfterRestart(StopAction stopAction) throws InterruptedException {

        EndpointCollection endpoints = new EndpointCollection();

        // Swapped for a fresh latch after the restart, so the second await can only be
        // satisfied by a poll that happened on the recreated executor.
        AtomicReference<CountDownLatch> polled = new AtomicReference<>(new CountDownLatch(1));

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        when(strategy.getEndpoints(anyMap(), anyBoolean())).thenAnswer(invocation -> {
            Map<EndpointsSelector, EndpointCollection> results = new HashMap<>();
            results.put(EndpointsType.ClusterEndpoint, endpoints);
            polled.get().countDown();
            return results;
        });

        ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

        GremlinClient client = mock(GremlinClient.class);

        Collection<RefreshTask> tasks = Collections.singletonList(
                new RefreshTask(client, EndpointsType.ClusterEndpoint)
        );

        try {
            agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);

            assertTrue("Agent should poll before being stopped",
                    polled.get().await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            stopAction.stop(agent);

            polled.set(new CountDownLatch(1));

            agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);

            assertTrue("Agent should resume polling after being restarted",
                    polled.get().await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } finally {
            agent.stop();
        }

        verify(client, atLeastOnce()).refreshEndpoints(endpoints);
    }

    @Test
    public void shouldSupportAwakeAfterStop() throws Exception {

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

        GremlinClient client = mock(GremlinClient.class);

        Collection<RefreshTask> tasks = Collections.singletonList(
                new RefreshTask(client, EndpointsType.ClusterEndpoint)
        );

        agent.startPollingNeptuneAPI(tasks, 1, TimeUnit.SECONDS);
        agent.stop();

        try {
            agent.awake();
        } finally {
            agent.stop();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIfStartedWhileAlreadyRunning() {

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

        GremlinClient client = mock(GremlinClient.class);

        Collection<RefreshTask> tasks = Collections.singletonList(
                new RefreshTask(client, EndpointsType.ClusterEndpoint)
        );

        try {
            agent.startPollingNeptuneAPI(tasks, 1, TimeUnit.SECONDS);
            agent.startPollingNeptuneAPI(tasks, 1, TimeUnit.SECONDS);
        } finally {
            agent.stop();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowIfClusterMetadataPollingStartedWhileAlreadyRunning() {

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

        OnNewClusterMetadata onNewClusterMetadata = metadata -> {
        };

        try {
            agent.startPollingNeptuneAPI(onNewClusterMetadata, 1, TimeUnit.SECONDS);
            agent.startPollingNeptuneAPI(onNewClusterMetadata, 1, TimeUnit.SECONDS);
        } finally {
            agent.stop();
        }
    }
}
