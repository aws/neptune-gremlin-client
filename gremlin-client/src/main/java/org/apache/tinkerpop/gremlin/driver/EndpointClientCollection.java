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

import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.driver.ApprovalResult.REJECTED_REASON_ANNOTATION;

class EndpointClientCollection implements Iterable<EndpointClient> {
    private final List<EndpointClient> endpointClients;
    private final EndpointCollection rejectedEndpoints;
    private final AtomicReference<Map<String, EndpointClientMetrics>> metrics;

    private final long startMillis = System.currentTimeMillis();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private static final Logger logger = LoggerFactory.getLogger(EndpointClientCollection.class);

    EndpointClientCollection() {
        this(new EndpointCollection());
    }

    EndpointClientCollection(EndpointCollection rejectedEndpoints) {
        this(new ArrayList<>(), rejectedEndpoints);
    }

    EndpointClientCollection(List<EndpointClient> endpointClients) {
        this(endpointClients, new EndpointCollection());
    }

    EndpointClientCollection(List<EndpointClient> endpointClients, EndpointCollection rejectedEndpoints) {
        this.rejectedEndpoints = rejectedEndpoints;
        this.endpointClients = endpointClients;
        this.metrics = initMetrics(endpointClients);
    }

    private AtomicReference<Map<String, EndpointClientMetrics>> initMetrics(List<EndpointClient> endpointClients) {
        Map<String, EndpointClientMetrics> endpointClientMetrics = new HashMap<>();
        for (EndpointClient endpointClient : endpointClients) {
            String address = endpointClient.endpoint().getAddress();
            endpointClientMetrics.put(address, new EndpointClientMetrics(address));
        }
        return new AtomicReference<>(endpointClientMetrics);
    }

    public List<EndpointClient> getSurvivingEndpointClients(EndpointCollection acceptedEndpoints) {
        List<EndpointClient> results = new ArrayList<>();
        for (EndpointClient endpointClient : endpointClients) {
            Endpoint endpoint = endpointClient.endpoint();
            if (acceptedEndpoints.containsEndpoint(endpoint)) {
                logger.info("Retaining client for {}", endpoint.getAddress());
                results.add(endpointClient);
            }
        }
        return results;
    }

    public Connection chooseConnection(RequestMessage msg, ChooseEndpointStrategy strategy) throws TimeoutException {

        long startMillis = System.currentTimeMillis();

        EndpointClient endpointClient = strategy.choose(this);
        String address = endpointClient.endpoint().getAddress();

        if (!endpointClient.isAvailable()){
            logger.debug("No connections available for {}", address);
            executorService.submit(() -> metrics.get().get(address).unavailable(startMillis));
            return null;
        }

        try {
            Connection connection = endpointClient.client().chooseConnection(msg);
            if (connection.isClosing()) {
                logger.debug("Connection is closing: {}", address);
                executorService.submit(() -> metrics.get().get(address).closing(startMillis));
                return null;
            }
            if (connection.isDead()) {
                logger.debug("Connection is dead: {}", address);
                executorService.submit(() -> metrics.get().get(address).dead(startMillis));
                return null;
            }
            executorService.submit(() -> metrics.get().get(address).succeeded(startMillis));
            return connection;
        } catch (NullPointerException e) {
            logger.debug("NullPointerException: {}", address, e);
            executorService.submit(() -> metrics.get().get(address).npe(startMillis));
            return null;
        } catch (NoHostAvailableException e) {
            logger.debug("No connection available: {}", address, e);
            executorService.submit(() -> metrics.get().get(address).nha(startMillis));
            return null;
        }
    }

    public EndpointClient get(int index){
        return endpointClients.get(index);
    }

    public int size() {
        return endpointClients.size();
    }

    public boolean isEmpty() {
        return endpointClients.isEmpty();
    }

    @Override
    public Iterator<EndpointClient> iterator() {
        return endpointClients.iterator();
    }

    public Stream<EndpointClient> stream() {
        return endpointClients.stream();
    }

    public EndpointCollection endpoints(){
        List<Endpoint> endpoints = endpointClients.stream()
                .map(EndpointClient::endpoint)
                .collect(Collectors.toList());
        return new EndpointCollection(endpoints);
    }

    public boolean hasRejectedEndpoints() {
        return !rejectedEndpoints.isEmpty();
    }

    public Collection<String> rejectionReasons() {
        return rejectedEndpoints.stream()
                .map(e -> e.getAnnotations().getOrDefault(REJECTED_REASON_ANNOTATION, "unknown"))
                .collect(Collectors.toSet());
    }

    public void close(){
        executorService.shutdownNow();
        Map<String, EndpointClientMetrics> endpointClientMetrics = metrics.get();

        long totalConnectionAttempts = 0L;
        for (EndpointClientMetrics clientMetrics : endpointClientMetrics.values()) {
            totalConnectionAttempts += clientMetrics.total();
        }

        logger.info("Connection metrics: [duration: {}ms, totalConnectionAttempts:{}, endpoints: [{}]]",
                System.currentTimeMillis() - startMillis,
                totalConnectionAttempts,
                endpointClientMetrics.values().stream()
                        .map(EndpointClientMetrics::toString)
                        .collect(Collectors.joining(", ")));
    }
}
