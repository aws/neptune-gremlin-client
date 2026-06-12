/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
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
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ClusterEndpointsRefreshAgentRestartTest {

    @Test
    public void shouldSupportStopAndRestart() {

        ClusterEndpointsFetchStrategy strategy = mock(ClusterEndpointsFetchStrategy.class);
        ClusterEndpointsRefreshAgent agent = new ClusterEndpointsRefreshAgent(strategy);

        GremlinClient client = mock(GremlinClient.class);

        Collection<RefreshTask> tasks = Collections.singletonList(
                new RefreshTask(client, EndpointsType.ClusterEndpoint)
        );

        agent.startPollingNeptuneAPI(tasks, 1, TimeUnit.SECONDS);
        agent.stop();

        try {
            agent.startPollingNeptuneAPI(tasks, 1, TimeUnit.SECONDS);
        } catch (IllegalStateException | java.util.concurrent.RejectedExecutionException e) {
            fail("Should be able to restart after stop, but got: " + e.getMessage());
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
}