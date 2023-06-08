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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.driver.ApprovalResult.REJECTED_REASON_ANNOTATION;

class EndpointClientCollection implements Iterable<EndpointClient> {
    private final List<EndpointClient> endpointClients;
    private final List<EndpointClient> selectableEndpointClients;
    private final EndpointCollection rejectedEndpoints;
    private final EndpointSelectionStrategy endpointSelectionStrategy;

    private static final Logger logger = LoggerFactory.getLogger(EndpointClientCollection.class);

    EndpointClientCollection(List<EndpointClient> endpointClients, EndpointSelectionStrategy endpointSelectionStrategy) {
        this(endpointClients, new EndpointCollection(), endpointSelectionStrategy);
    }

    EndpointClientCollection(List<EndpointClient> endpointClients,
                             EndpointCollection rejectedEndpoints,
                             EndpointSelectionStrategy endpointSelectionStrategy) {
        this.rejectedEndpoints = rejectedEndpoints;
        this.endpointClients = endpointClients;
        this.endpointSelectionStrategy = endpointSelectionStrategy;
        this.selectableEndpointClients = endpointSelectionStrategy.init(endpointClients);
    }

    public List<EndpointClient> getSurvivingEndpointClients(EndpointCollection acceptedEndpoints) {
        List<EndpointClient> results = new ArrayList<>();
        for (EndpointClient endpointClient : endpointClients) {
            Endpoint endpoint = endpointClient.endpoint();
            if (acceptedEndpoints.containsEndpoint(endpoint)) {
                logger.info("Retaining client for {}", endpoint.getAddress());
                results.add(new EndpointClient(acceptedEndpoints.get(endpoint.getAddress()), endpointClient.client()));
            }
        }
        return results;
    }

    public Connection chooseConnection(RequestMessage msg) throws TimeoutException {

        EndpointClient endpointClient = endpointSelectionStrategy.select(msg, selectableEndpointClients);

        String address = endpointClient.endpoint().getAddress();
        try {
            Connection connection = endpointClient.client().chooseConnection(msg);
            if (connection.isClosing()) {
                logger.debug("Connection is closing: {}", address);
                return null;
            }
            if (connection.isDead()) {
                logger.debug("Connection is dead: {}", address);
                return null;
            }
            return connection;
        } catch (NullPointerException e) {
            logger.debug("NullPointerException: {}", address, e);
            return null;
        } catch (NoHostAvailableException e) {
            logger.debug("No connection available: {}", address, e);
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
        List<Endpoint> endpoints = endpointClients.stream().map(e -> e.endpoint()).collect(Collectors.toList());
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
}
