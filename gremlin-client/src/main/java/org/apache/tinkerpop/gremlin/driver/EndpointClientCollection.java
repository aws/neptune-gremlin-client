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
    private final List<EndpointClient> endpointClients;
    private final EndpointCollection rejectedEndpoints;
    private final Map<String, EndpointConnectionMetrics> connectionMetrics;
    private final RequestMetrics requestMetrics;
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
        this.connectionMetrics = initMetrics(endpointClients);
        this.requestMetrics = initRequestMetrics(endpointClients);
    }

    private RequestMetrics initRequestMetrics(List<EndpointClient> endpointClients) {
        Map<String, EndpointRequestMetrics> requestMetrics = new HashMap<>();
        for (EndpointClient endpointClient : endpointClients) {
            String address = endpointClient.endpoint().getAddress();
            requestMetrics.put(address, new EndpointRequestMetrics(address));
        }
        return new RequestMetrics(requestMetrics);
    }

    private Map<String, EndpointConnectionMetrics> initMetrics(List<EndpointClient> endpointClients) {
        Map<String, EndpointConnectionMetrics> endpointClientMetrics = new HashMap<>();
        for (EndpointClient endpointClient : endpointClients) {
            String address = endpointClient.endpoint().getAddress();
            endpointClientMetrics.put(address, new EndpointConnectionMetrics(address));
        }
        return endpointClientMetrics;
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

        UUID traceId = msg.getRequestId();

        long startMillis = System.currentTimeMillis();

        EndpointClient endpointClient = strategy.choose(this);
        String address = endpointClient.endpoint().getAddress();

        if (!endpointClient.isAvailable()) {
            logger.debug("No connections available for {}", address);
            try {
                executorService.submit(() -> connectionMetrics.get(address).unavailable(startMillis));
            } catch (RejectedExecutionException e) {
                // Do nothing
            }
            return null;
        }

        try {
            Connection connection = endpointClient.client().chooseConnection(msg);
            if (connection.isClosing()) {
                logger.debug("Connection is closing: {}", address);
                try {
                    executorService.submit(() -> connectionMetrics.get(address).closing(startMillis));
                } catch (RejectedExecutionException e) {
                    // Do nothing
                }
                return null;
            }
            if (connection.isDead()) {
                logger.debug("Connection is dead: {}", address);
                try {
                    executorService.submit(() -> connectionMetrics.get(address).dead(startMillis));
                } catch (RejectedExecutionException e) {
                    // Do nothing
                }
                return null;
            }
            try {
                executorService.submit(() -> {
                    try {
                        if (connectionMetrics.containsKey(address)) {
                            connectionMetrics.get(address).succeeded(startMillis);
                        }
                        requestMetrics.registerAddressForTraceId(address, traceId);
                    } catch (Exception e) {
                        logger.error("Error while submitting metrics", e);
                    }
                });
            } catch (RejectedExecutionException e) {
                // Do nothing
            }
            return connection;
        } catch (NullPointerException e) {
            logger.debug("NullPointerException: {}", address, e);
            try {
                executorService.submit(() -> connectionMetrics.get(address).npe(startMillis));
            } catch (RejectedExecutionException ex) {
                // Do nothing
            }
            return null;
        } catch (NoHostAvailableException e) {
            logger.debug("No connection available: {}", address, e);
            try {
                executorService.submit(() -> connectionMetrics.get(address).nha(startMillis));
            } catch (RejectedExecutionException ex) {
                // Do nothing
            }
            return null;
        }
    }

    public EndpointClient get(int index) {
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

    public EndpointCollection endpoints() {
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

    public void close() {
        executorService.shutdown();
        try {
            executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("Interrupted while shutting down EndpointClientCollection", e);
        }

        Map<String, EndpointRequestMetrics> endpointRequestMetrics = requestMetrics.endpointRequestMetrics();

        long totalConnectionAttempts = 0;
        for (EndpointConnectionMetrics cm : connectionMetrics.values()) {
            totalConnectionAttempts += cm.total();
        }

        long totalRequests = 0;
        for (EndpointRequestMetrics rm : endpointRequestMetrics.values()) {
            totalRequests += rm.count();
        }
        for (Map.Entry<String, EndpointRequestMetrics> rm : endpointRequestMetrics.entrySet()) {
            totalRequests += rm.getValue().count();
        }


        long duration = System.currentTimeMillis() - startMillis;

        logger.info("Connection metrics: [duration: {}ms, totalConnectionAttempts:{}, endpoints: [{}]]",
                duration,
                totalConnectionAttempts,
                connectionMetrics.values().stream()
                        .map(EndpointConnectionMetrics::toString)
                        .collect(Collectors.joining(", ")));

        logger.info("Request metrics: [duration: {}ms, totalRequests:{}, endpoints: [{}]]",
                duration,
                totalRequests,
                endpointRequestMetrics.values().stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(", ")));
    }

    public void registerDurationForTraceId(UUID traceId, long durationMillis) {
        executorService.submit(() -> {
            requestMetrics.registerDurationForTraceId(traceId, durationMillis);
        });
    }
}
