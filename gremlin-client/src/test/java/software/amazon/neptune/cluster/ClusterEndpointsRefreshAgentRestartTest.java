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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterEndpointsRefreshAgentRestartTest {

    private static final long AWAIT_TIMEOUT_SECONDS = 10;
    private static final long POLLING_DELAY_MILLIS = 50;
    private static final int RACE_ITERATIONS = 200;

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

    /**
     * Waking a stopped agent cannot resume polling, so it must report the problem rather than recreate an
     * executor whose thread would never poll and would outlive the stop.
     */
    @Test(expected = IllegalStateException.class)
    public void shouldThrowIfAwokenAfterStop() throws Exception {

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

        GremlinClient client = mock(GremlinClient.class);

        Collection<RefreshTask> tasks = Collections.singletonList(
                new RefreshTask(client, EndpointsType.ClusterEndpoint)
        );

        agent.startPollingNeptuneAPI(tasks, 1, TimeUnit.SECONDS);
        agent.stop();

        agent.awake();
    }

    @Test
    public void shouldSupportAwakeAfterRestart() throws Exception {

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

        GremlinClient client = mock(GremlinClient.class);

        Collection<RefreshTask> tasks = Collections.singletonList(
                new RefreshTask(client, EndpointsType.ClusterEndpoint)
        );

        agent.startPollingNeptuneAPI(tasks, 1, TimeUnit.SECONDS);
        agent.stop();
        agent.startPollingNeptuneAPI(tasks, 1, TimeUnit.SECONDS);

        try {
            agent.awake();
        } finally {
            agent.stop();
        }
    }

    /**
     * A stop that cannot confirm termination must leave the agent marked as running, so that a restart is
     * rejected rather than adding a second poller alongside the one that is still going.
     */
    @Test
    public void shouldRejectRestartWhenTerminationIsNotConfirmed() throws InterruptedException {

        CountDownLatch polling = new CountDownLatch(1);
        CountDownLatch releasePolling = new CountDownLatch(1);

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        when(strategy.getEndpoints(anyMap(), anyBoolean())).thenAnswer(invocation -> {
            polling.countDown();
            boolean released = false;
            while (!released) {
                try {
                    released = releasePolling.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    // Deliberately swallow the interrupt from shutdownNow(), standing in for a fetch that
                    // does not respond to interruption, so the stop below cannot confirm termination.
                }
            }
            return new HashMap<EndpointsSelector, EndpointCollection>();
        });

        ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

        GremlinClient client = mock(GremlinClient.class);

        Collection<RefreshTask> tasks = Collections.singletonList(
                new RefreshTask(client, EndpointsType.ClusterEndpoint)
        );

        try {
            agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);

            assertTrue("Agent should be polling before being stopped",
                    polling.await(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            assertFalse("stop should report that the polling task did not terminate",
                    agent.stop(POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS));

            try {
                agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);
                fail("Restarting before the polling task has terminated should throw");
            } catch (IllegalStateException e) {
                // Expected.
            }

            releasePolling.countDown();

            assertTrue("A subsequent stop should confirm termination once the task has finished",
                    agent.stop(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

            // The agent is restartable again now that termination has been confirmed.
            agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } finally {
            releasePolling.countDown();
            agent.stop();
        }
    }

    /**
     * A start that fails to schedule must not leave the agent marked as running, otherwise it can never be
     * started again.
     */
    @Test
    public void shouldRemainStartableWhenSchedulingIsRejected() {

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

        GremlinClient client = mock(GremlinClient.class);

        Collection<RefreshTask> tasks = Collections.singletonList(
                new RefreshTask(client, EndpointsType.ClusterEndpoint)
        );

        try {
            try {
                // A non-positive delay is rejected by scheduleWithFixedDelay.
                agent.startPollingNeptuneAPI(tasks, 0, TimeUnit.MILLISECONDS);
                fail("A non-positive delay should be rejected");
            } catch (IllegalArgumentException e) {
                // Expected.
            }

            // No polling task was scheduled, so the agent must still be startable.
            agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);
        } finally {
            agent.stop();
        }
    }

    /**
     * Races a start against a stop repeatedly. Whichever wins, the agent must end up in a consistent
     * state: if a start reported success then a poller is live and a further start must be rejected,
     * and if the agent is not running a start must be accepted.
     * <p>
     * This is a best-effort stress test rather than a deterministic one; the interleaving it guards
     * against is narrow, so passing does not prove the absence of a race.
     */
    @Test
    public void shouldKeepRunningStateConsistentWhenStartRacesStop() throws Exception {

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        when(strategy.getEndpoints(anyMap(), anyBoolean()))
                .thenAnswer(invocation -> new HashMap<EndpointsSelector, EndpointCollection>());

        GremlinClient client = mock(GremlinClient.class);

        Collection<RefreshTask> tasks = Collections.singletonList(
                new RefreshTask(client, EndpointsType.ClusterEndpoint)
        );

        ExecutorService racers = Executors.newFixedThreadPool(2);

        try {
            for (int i = 0; i < RACE_ITERATIONS; i++) {

                ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

                try {
                    agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);

                    CountDownLatch start = new CountDownLatch(1);

                    Future<Boolean> started = racers.submit(() -> {
                        start.await();
                        try {
                            agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);
                            return true;
                        } catch (IllegalStateException e) {
                            return false;
                        }
                    });

                    Future<?> stopped = racers.submit(() -> {
                        start.await();
                        agent.stop();
                        return null;
                    });

                    start.countDown();

                    boolean startSucceeded = started.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    stopped.get(AWAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                    if (startSucceeded) {
                        // A poller was scheduled, so the agent must still consider itself running. If the
                        // flag were cleared by the racing stop, this second start would silently add a
                        // poller alongside the live one.
                        try {
                            agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);
                            fail("Start should be rejected while a polling task is live");
                        } catch (IllegalStateException e) {
                            // Expected.
                        }
                    } else {
                        // The start lost the race, so the stop fully took effect and a restart must work.
                        agent.startPollingNeptuneAPI(tasks, POLLING_DELAY_MILLIS, TimeUnit.MILLISECONDS);
                    }
                } finally {
                    agent.stop(0, TimeUnit.MILLISECONDS);
                }
            }
        } finally {
            racers.shutdownNow();
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
