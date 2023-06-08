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

import org.apache.tinkerpop.gremlin.driver.DatabaseEndpoint;
import org.apache.tinkerpop.gremlin.driver.EndpointClient;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.lang.Math.log;
import static org.junit.Assert.assertEquals;

public class CloudWatchMetricsBasedEndpointsSelectionStrategyTest {

    @Test
    @Ignore
    public void shouldAssignEqualNumberOfSlotsWhenAllMetricsZero() {
        DatabaseEndpoint endpoint1 = new DatabaseEndpoint().withAddress("address-1");
        endpoint1.setMetric(Metric.CPUUtilization.name(), 0.0);
        endpoint1.setMetric(Metric.MainRequestQueuePendingRequests.name(), 0.0);

        DatabaseEndpoint endpoint2 = new DatabaseEndpoint().withAddress("address-2");
        endpoint2.setMetric(Metric.CPUUtilization.name(), 0.0);
        endpoint2.setMetric(Metric.MainRequestQueuePendingRequests.name(), 0.0);

        DatabaseEndpoint endpoint3 = new DatabaseEndpoint().withAddress("address-3");
        endpoint3.setMetric(Metric.CPUUtilization.name(), 0.0);
        endpoint3.setMetric(Metric.MainRequestQueuePendingRequests.name(), 0.0);

        List<EndpointClient> endpointClients = Arrays.asList(
                new EndpointClient(endpoint1, null),
                new EndpointClient(endpoint2, null),
                new EndpointClient(endpoint3, null)
        );

        CloudWatchMetricsBasedEndpointsSelectionStrategy strategy = new CloudWatchMetricsBasedEndpointsSelectionStrategy();
        List<EndpointClient> selectableEndpoints = strategy.init(endpointClients);

        assertEquals(15, selectableEndpoints.size());
        assertEquals(5, selectableEndpoints.stream().filter(c -> c.endpoint().getAddress().equals("address-1")).count());
        assertEquals(5, selectableEndpoints.stream().filter(c -> c.endpoint().getAddress().equals("address-2")).count());
        assertEquals(5, selectableEndpoints.stream().filter(c -> c.endpoint().getAddress().equals("address-3")).count());
    }

    @Test
    @Ignore
    public void whenQueueDepthIsLowerCpuWins() {
        DatabaseEndpoint endpoint1 = new DatabaseEndpoint().withAddress("address-1");
        endpoint1.setMetric(Metric.CPUUtilization.name(), 50.0);
        endpoint1.setMetric(Metric.MainRequestQueuePendingRequests.name(), 0.0);

        DatabaseEndpoint endpoint2 = new DatabaseEndpoint().withAddress("address-2");
        endpoint2.setMetric(Metric.CPUUtilization.name(), 25.0);
        endpoint2.setMetric(Metric.MainRequestQueuePendingRequests.name(), 0.0);

        List<EndpointClient> endpointClients = Arrays.asList(
                new EndpointClient(endpoint1, null),
                new EndpointClient(endpoint2, null)
        );

        CloudWatchMetricsBasedEndpointsSelectionStrategy strategy = new CloudWatchMetricsBasedEndpointsSelectionStrategy();
        List<EndpointClient> selectableEndpoints = strategy.init(endpointClients);

        assertEquals(10, selectableEndpoints.size());
        assertEquals(4, selectableEndpoints.stream().filter(c -> c.endpoint().getAddress().equals("address-1")).count());
        assertEquals(6, selectableEndpoints.stream().filter(c -> c.endpoint().getAddress().equals("address-2")).count());
    }

    @Test
    @Ignore
    public void whenCpuIsEqualQueueDepthWins() {
        DatabaseEndpoint endpoint1 = new DatabaseEndpoint().withAddress("address-1");
        endpoint1.setMetric(Metric.CPUUtilization.name(), 50.0);
        endpoint1.setMetric(Metric.MainRequestQueuePendingRequests.name(), 7000.0);

        DatabaseEndpoint endpoint2 = new DatabaseEndpoint().withAddress("address-2");
        endpoint2.setMetric(Metric.CPUUtilization.name(), 50.0);
        endpoint2.setMetric(Metric.MainRequestQueuePendingRequests.name(), 0.0);

        List<EndpointClient> endpointClients = Arrays.asList(
                new EndpointClient(endpoint1, null),
                new EndpointClient(endpoint2, null)
        );

        CloudWatchMetricsBasedEndpointsSelectionStrategy strategy = new CloudWatchMetricsBasedEndpointsSelectionStrategy();
        List<EndpointClient> selectableEndpoints = strategy.init(endpointClients);

        assertEquals(11, selectableEndpoints.size());
        assertEquals(2, selectableEndpoints.stream().filter(c -> c.endpoint().getAddress().equals("address-1")).count());
        assertEquals(9, selectableEndpoints.stream().filter(c -> c.endpoint().getAddress().equals("address-2")).count());
    }

    @Test
    public void deleteMe(){
        System.out.println(18/8192);
        System.out.println(formatNumber(18.0/8192));
        System.out.println(log(0.0));
        System.out.println(formatNumber(0.0003));
        System.out.println(formatNumber(0.001));
        System.out.println(formatNumber(0.01));
        System.out.println(formatNumber(0.1));
        System.out.println(formatNumber(0.2));
        System.out.println(formatNumber(0.3));
        System.out.println(formatNumber(0.4));
        System.out.println(formatNumber(0.5));
        System.out.println(formatNumber(0.6));
        System.out.println(formatNumber(0.7));
        System.out.println(formatNumber(0.8));
        System.out.println(formatNumber(0.9));
    }

    private int formatNumber(double value){
        return (int) Math.abs(Math.ceil(log(value) * 10));
    }
}