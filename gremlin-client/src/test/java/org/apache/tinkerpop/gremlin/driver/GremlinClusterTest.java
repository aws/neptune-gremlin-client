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

package org.apache.tinkerpop.gremlin.driver;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.junit.Test;

import java.net.ConnectException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class GremlinClusterTest {

    @Test
    public void shouldCloseAllClustersWhenConnectFailsWithNoHostAvailable() throws Exception {
        int port = refusedPort();
        List<Cluster> created = new CopyOnWriteArrayList<>();
        Set<Thread> threadsBefore = gremlinDriverThreads();

        GremlinCluster gremlinCluster = gremlinCluster(port, created, new HashMap<>());

        try {
            gremlinCluster.connect();
            fail("Expected connect to fail against a refused port");
        } catch (NoHostAvailableException e) {
            // expected
        } finally {
            gremlinCluster.close();
        }

        assertEquals("parent cluster and one endpoint cluster", 2, created.size());
        for (Cluster cluster : created) {
            assertTrue("cluster left open after a failed connect: " + cluster, cluster.isClosed());
        }
        assertNoNewGremlinDriverThreads(threadsBefore);
    }

    @Test
    public void shouldCloseAllClustersWhenEveryEndpointIsIgnoredAsUnavailable() throws Exception {
        int port = refusedPort();
        List<Cluster> created = new CopyOnWriteArrayList<>();
        Set<Thread> threadsBefore = gremlinDriverThreads();

        // Treat this endpoint's connect failure as ignorable, so connect() takes the
        // EndpointsUnavailableException path instead of rethrowing the driver exception.
        Map<Class<? extends Exception>, Set<String>> ignorable = new HashMap<>();
        ignorable.put(NoHostAvailableException.class, Collections.singleton(connectFailureMessage(port)));

        GremlinCluster gremlinCluster = gremlinCluster(port, created, ignorable);

        try {
            gremlinCluster.connect();
            fail("Expected connect to fail against a refused port");
        } catch (EndpointsUnavailableException e) {
            // expected
        } finally {
            gremlinCluster.close();
        }

        assertEquals("parent cluster and one endpoint cluster", 2, created.size());
        for (Cluster cluster : created) {
            assertTrue("cluster left open after a failed connect: " + cluster, cluster.isClosed());
        }
        assertNoNewGremlinDriverThreads(threadsBefore);
    }

    private static GremlinCluster gremlinCluster(int port,
                                                 List<Cluster> created,
                                                 Map<Class<? extends Exception>, Set<String>> ignorable) {
        ClusterFactory clusterFactory = endpoints -> {
            Cluster cluster = clusterBuilder(port, endpoints).create();
            created.add(cluster);
            return cluster;
        };
        return new GremlinCluster(
                Collections.singletonList(new DatabaseEndpoint().withAddress("127.0.0.1")),
                clusterFactory,
                new EndpointStrategies(EndpointFilter.NULL_ENDPOINT_FILTER),
                new AcquireConnectionConfig(1000, -1, null, 5000, 5),
                new MetricsConfig(false, new MetricsHandlerCollection()),
                ignorable);
    }

    private static Cluster.Builder clusterBuilder(int port, EndpointCollection endpoints) {
        Cluster.Builder builder = Cluster.build()
                .port(port)
                .nioPoolSize(1)
                .workerPoolSize(1)
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1);
        if (endpoints != null) {
            for (Endpoint endpoint : endpoints) {
                builder.addContactPoint(endpoint.getAddress());
            }
        }
        return builder;
    }

    /** The exact message the driver gives for a refused connect to this port. */
    private static String connectFailureMessage(int port) {
        Cluster probe = clusterBuilder(port, new EndpointCollection(
                Collections.singletonList(new DatabaseEndpoint().withAddress("127.0.0.1")))).create();
        try {
            probe.connect().init();
            throw new AssertionError("Expected connect to fail against a refused port");
        } catch (NoHostAvailableException e) {
            // The message is only stable while the failure is a plain refused connect.
            assertTrue("unexpected probe failure: " + e, ExceptionUtils.getRootCause(e) instanceof ConnectException);
            return e.getMessage();
        } finally {
            probe.close();
        }
    }

    private static int refusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static Set<Thread> gremlinDriverThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(t -> t.isAlive() && t.getName().startsWith("gremlin-driver-"))
                .collect(Collectors.toSet());
    }

    /** No gremlin-driver thread that was not already running before the test is still alive. */
    private static void assertNoNewGremlinDriverThreads(Set<Thread> before) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        Set<Thread> leaked = newThreads(before);
        while (!leaked.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            leaked = newThreads(before);
        }
        assertTrue("gremlin-driver threads leaked: " + leaked, leaked.isEmpty());
    }

    private static Set<Thread> newThreads(Set<Thread> before) {
        Set<Thread> now = gremlinDriverThreads();
        now.removeAll(before);
        return now;
    }
}
