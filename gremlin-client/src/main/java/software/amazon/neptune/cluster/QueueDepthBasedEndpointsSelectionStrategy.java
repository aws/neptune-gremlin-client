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

import org.apache.tinkerpop.gremlin.driver.Endpoint;
import org.apache.tinkerpop.gremlin.driver.EndpointClient;
import org.apache.tinkerpop.gremlin.driver.EndpointSelectionStrategy;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.log;

public class QueueDepthBasedEndpointsSelectionStrategy implements EndpointSelectionStrategy {

    private static final Logger logger = LoggerFactory.getLogger(QueueDepthBasedEndpointsSelectionStrategy.class);

    private static final int MAX_QUEUE_DEPTH = 8192;

    private final AtomicLong index = new AtomicLong(0);

    @Override
    public EndpointClient select(RequestMessage msg, List<EndpointClient> endpointClients) {
        return endpointClients.get((int) (index.getAndIncrement() % endpointClients.size()));
    }

    @Override
    public List<EndpointClient> init(List<EndpointClient> endpointClients) {

        if (endpointClients.isEmpty()){
            return endpointClients;
        }

        List<EndpointClient> endpointSlots = new ArrayList<>();

        for (int i = 0; i < 10; i++){
            for (EndpointClient endpointClient : endpointClients) {
                endpointSlots.add(endpointClient);
            }
        }

        for (int i = 20; i > 0; i --){
            int targetScore = i * 5;
            for (EndpointClient endpointClient : endpointClients) {
                Map<String, Double> metrics = endpointClient.endpoint().getMetrics();
                int score = calculateScore(metrics.getOrDefault(Metric.MainRequestQueuePendingRequests.name(), 0.0)/MAX_QUEUE_DEPTH);
                if (score >= targetScore){
                    endpointSlots.add(endpointClient);
                }
            }
        }

        for (EndpointClient endpointClient : endpointClients) {
            Endpoint endpoint = endpointClient.endpoint();
            long slots = endpointSlots.stream().filter(e -> e.endpoint().getAddress().equals(endpoint.getAddress())).count();
            logger.info("{}, slots: {}, cpu: {}, queue-depth: {}",
                    endpoint.getAddress(),
                    slots,
                    formatDefault(endpoint.getMetrics(), Metric.CPUUtilization, 50.0),
                    formatDefault(endpoint.getMetrics(), Metric.MainRequestQueuePendingRequests, 0.0));
        }

        return endpointSlots;
    }

    private int calculateScore(double value){
        if (value == 0){
            return 100;
        }
        return (int) Math.abs(Math.ceil(log(value) * 10));
    }

    private String formatDefault(Map<String, Double> metrics, Metric metric, double defaultValue){
        if (metrics.containsKey(metric.name())){
            return String.valueOf(metrics.get(metric.name()));
        } else {
            return String.format("[default: %s]", defaultValue);
        }
    }
}
