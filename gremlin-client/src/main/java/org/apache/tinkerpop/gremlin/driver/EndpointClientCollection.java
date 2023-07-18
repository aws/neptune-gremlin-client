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
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.driver.ApprovalResult.REJECTED_REASON_ANNOTATION;

class EndpointClientCollection implements Iterable<EndpointClient> {

    public static Builder builder(){
        return new Builder();
    }

    private final List<EndpointClient> endpointClients;
    private final EndpointCollection rejectedEndpoints;
    private final boolean collectMetrics;
    private final ConnectionMetricsCollector connectionMetrics;
    private final RequestMetricsCollector requestMetrics;
    private final long startMillis = System.currentTimeMillis();

    private final ExecutorService executorService;

    private static final Logger logger = LoggerFactory.getLogger(EndpointClientCollection.class);

    EndpointClientCollection(Builder builder) {
        this.rejectedEndpoints = builder.getRejectedEndpoints();
        this.endpointClients = builder.getEndpointClients();
        this.collectMetrics = builder.collectMetrics();
        this.executorService = collectMetrics ? Executors.newSingleThreadExecutor() : null;
        this.connectionMetrics = collectMetrics ? initConnectionMetrics(endpointClients) : null;
        this.requestMetrics = collectMetrics ? initRequestMetrics(endpointClients) : null;
    }

    EndpointClientCollection() {
        this(new Builder());
    }

    private RequestMetricsCollector initRequestMetrics(List<EndpointClient> endpointClients) {
        Map<String, EndpointRequestMetrics> requestMetrics = new ConcurrentHashMap<>();
        for (EndpointClient endpointClient : endpointClients) {
            String address = endpointClient.endpoint().getAddress();
            requestMetrics.put(address, new EndpointRequestMetrics(address));
        }
        return new RequestMetricsCollector(requestMetrics);
    }

    private ConnectionMetricsCollector initConnectionMetrics(List<EndpointClient> endpointClients) {
        Map<String, EndpointConnectionMetrics> endpointClientMetrics = new ConcurrentHashMap<>();
        for (EndpointClient endpointClient : endpointClients) {
            String address = endpointClient.endpoint().getAddress();
            endpointClientMetrics.put(address, new EndpointConnectionMetrics(address));
        }
        return new ConnectionMetricsCollector(endpointClientMetrics);
    }

    List<EndpointClient> getSurvivingEndpointClients(EndpointCollection acceptedEndpoints) {
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

    Connection chooseConnection(RequestMessage msg, ChooseEndpointStrategy strategy) throws TimeoutException {

        UUID traceId = msg.getRequestId();

        long startMillis = System.currentTimeMillis();

        EndpointClient endpointClient = strategy.choose(this);
        String address = endpointClient.endpoint().getAddress();

        if (!endpointClient.isAvailable()) {
            logger.debug("No connections available for {}", address);
            submitMetrics(() -> connectionMetrics.unavailable(address, startMillis));
            return null;
        }

        try {

            Connection connection = endpointClient.client().chooseConnection(msg);

            if (connection.isClosing()) {
                logger.debug("Connection is closing: {}", address);
                submitMetrics(() -> connectionMetrics.closing(address, startMillis));
                return null;
            }

            if (connection.isDead()) {
                logger.debug("Connection is dead: {}", address);
                submitMetrics(() -> connectionMetrics.dead(address, startMillis));
                return null;
            }

            submitMetrics(() -> {
                try {
                    connectionMetrics.succeeded(address, startMillis);
                    requestMetrics.registerAddressForTraceId(traceId, address);
                } catch (Exception e) {
                    logger.error("Error while submitting metrics", e);
                }
            });

            return connection;

        } catch (NullPointerException e) {
            logger.debug("NullPointerException: {}", address, e);
            submitMetrics(() -> connectionMetrics.npe(address, startMillis));
            return null;
        } catch (NoHostAvailableException e) {
            logger.debug("No connection available: {}", address, e);
            submitMetrics(() -> connectionMetrics.nha(address, startMillis));
            return null;
        }
    }

    EndpointClient get(int index) {
        return endpointClients.get(index);
    }

    int size() {
        return endpointClients.size();
    }

    boolean isEmpty() {
        return endpointClients.isEmpty();
    }

    @Override
    public Iterator<EndpointClient> iterator() {
        return endpointClients.iterator();
    }

    Stream<EndpointClient> stream() {
        return endpointClients.stream();
    }

    EndpointCollection endpoints() {
        List<Endpoint> endpoints = endpointClients.stream()
                .map(EndpointClient::endpoint)
                .collect(Collectors.toList());
        return new EndpointCollection(endpoints);
    }

    boolean hasRejectedEndpoints() {
        return !rejectedEndpoints.isEmpty();
    }

    Collection<String> rejectionReasons() {
        return rejectedEndpoints.stream()
                .map(e -> e.getAnnotations().getOrDefault(REJECTED_REASON_ANNOTATION, "unknown"))
                .collect(Collectors.toSet());
    }

    private void submitMetrics(Runnable runnable){
        if (collectMetrics) {
            try {
                executorService.submit(runnable);
            } catch (RejectedExecutionException ex) {
                // Do nothing
            }
        }
    }

    void close(MetricsHandler handler) {

        if (executorService != null) {
            executorService.shutdownNow();
        }

        if (!collectMetrics) {
            return;
        }

        if (handler != null){

            long duration = System.currentTimeMillis() - startMillis;

            ConnectionMetrics conMetrics = new ConnectionMetrics(
                    duration,
                    connectionMetrics.totalConnectionAttempts(),
                    connectionMetrics.metrics() );

            RequestMetrics reqMetrics = new RequestMetrics(
                    duration,
                    requestMetrics.totalRequests(),
                    requestMetrics.failedRequests(),
                    requestMetrics.droppedRequests(),
                    requestMetrics.skippedResponses(),
                    requestMetrics.metrics());

            handler.onMetricsPublished(conMetrics, reqMetrics);
        }
    }

    void registerDurationForTraceId(UUID traceId, long durationMillis, Throwable e) {
        submitMetrics(() -> requestMetrics.registerDurationForTraceId(traceId, durationMillis, e));
    }

    static class Builder {
        private List<EndpointClient> endpointClients = new ArrayList<>();
        private EndpointCollection rejectedEndpoints = new EndpointCollection();
        private boolean collectMetrics = false;

        private Builder(){

        }

        public Builder withEndpointClients(List<EndpointClient> endpointClients) {
            this.endpointClients = endpointClients;
            return this;
        }

        public Builder withRejectedEndpoints(EndpointCollection rejectedEndpoints) {
            this.rejectedEndpoints = rejectedEndpoints;
            return this;
        }

        public Builder setCollectMetrics(boolean collectMetrics) {
            this.collectMetrics = collectMetrics;
            return this;
        }

        List<EndpointClient> getEndpointClients() {
            return endpointClients;
        }

        EndpointCollection getRejectedEndpoints() {
            return rejectedEndpoints;
        }

        boolean collectMetrics() {
            return collectMetrics;
        }
    }
}
