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

import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class EndpointStrategies {
    private final AvailableEndpointFilter availableEndpointFilter;
    private final int maxAttemptsToAcquireConnection;
    private final int maxTimeToAcquireConnectionMillis;
    private final Supplier<EndpointCollection> onFailureToAcquireConnection;

    private static final Logger logger = LoggerFactory.getLogger(EndpointStrategies.class);

    public EndpointStrategies(AvailableEndpointFilter availableEndpointFilter,
                              int maxAttemptsToAcquireConnection,
                              int maxWaitBeforeRaisingUnsuccessfulConnectAttemptMillis,
                              Supplier<EndpointCollection> onFailureToAcquireConnection) {
        this.availableEndpointFilter = availableEndpointFilter;
        this.maxAttemptsToAcquireConnection = maxAttemptsToAcquireConnection;
        this.maxTimeToAcquireConnectionMillis = maxWaitBeforeRaisingUnsuccessfulConnectAttemptMillis;
        this.onFailureToAcquireConnection = onFailureToAcquireConnection;
    }

    public AvailableEndpointFilter availableEndpointFilter() {
        return availableEndpointFilter;
    }

    public UnsuccessfulConnectAttemptManager createUnsuccessfulConnectAttemptManager(GremlinClient gremlinClient){
        return new UnsuccessfulConnectAttemptManager(
                gremlinClient,
                maxAttemptsToAcquireConnection,
                maxTimeToAcquireConnectionMillis,
                onFailureToAcquireConnection);
    }
}
