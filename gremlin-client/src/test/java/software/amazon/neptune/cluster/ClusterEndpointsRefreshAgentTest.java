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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ClusterEndpointsRefreshAgentTest {

    @Test
    public void shouldRefreshEndpointsForEachClient() {

        GremlinClient client1 = mock(GremlinClient.class);
        GremlinClient client2 = mock(GremlinClient.class);
        GremlinClient client3 = mock(GremlinClient.class);
        GremlinClient client4 = mock(GremlinClient.class);

        EndpointCollection endpoints1 = new EndpointCollection();
        EndpointCollection endpoints2 = new EndpointCollection();

        Collection<RefreshTask> refreshTasks = Arrays.asList(
                new RefreshTask(client1, EndpointsType.ClusterEndpoint),
                new RefreshTask(client2, EndpointsType.ClusterEndpoint),
                new RefreshTask(client3, EndpointsType.ReaderEndpoint),
                new RefreshTask(client4, EndpointsType.ReaderEndpoint)

        );

        ClusterEndpointsRefreshAgent.EndpointsSupplier endpointsSupplier = selectors -> {
            HashMap<EndpointsSelector, EndpointCollection> results = new HashMap<>();
            results.put(EndpointsType.ClusterEndpoint, endpoints1);
            results.put(EndpointsType.ReaderEndpoint, endpoints2);
            return results;
        };

        ClusterEndpointsRefreshAgent.PollingCommand pollingCommand =
                new ClusterEndpointsRefreshAgent.PollingCommand(refreshTasks, endpointsSupplier);

        pollingCommand.run();

        verify(client1).refreshEndpoints(endpoints1);
        verify(client2).refreshEndpoints(endpoints1);
        verify(client3).refreshEndpoints(endpoints2);
        verify(client4).refreshEndpoints(endpoints2);
    }
}
