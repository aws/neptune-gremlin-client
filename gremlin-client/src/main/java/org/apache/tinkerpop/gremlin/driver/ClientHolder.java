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

import org.apache.tinkerpop.gremlin.driver.exception.ConnectionException;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

class ClientHolder {
    private final String endpoint;
    private final Client client;

    private static final Logger logger = LoggerFactory.getLogger(ClientHolder.class);

    ClientHolder(String endpoint, Client client) {
        this.endpoint = endpoint;
        this.client = client;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isAvailable() {
        return !client.getCluster().availableHosts().isEmpty();
    }

    public Connection chooseConnection(RequestMessage msg) throws TimeoutException, ConnectionException {
        try {
            Connection connection = client.chooseConnection(msg);
            if (connection.isClosing()) {
                logger.debug("Connection is closing: {}", endpoint);
                return null;
            }
            if (connection.isDead()) {
                logger.debug("Connection is dead: {}", endpoint);
                return null;
            }
            return connection;
        } catch (NullPointerException e) {
            logger.debug("NullPointerException: {}", endpoint, e);
            return null;
        } catch (NoHostAvailableException e){
            logger.debug("No connection available: {}", endpoint, e);
            return null;
        }
    }

    public CompletableFuture<Void> closeAsync() {
        return client.closeAsync();
    }

    public void init() {
        client.init();
    }
}
