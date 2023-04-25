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

import java.util.function.Supplier;

public class AcquireConnectionConfig {

    private final int maxWaitForConnection;
    private final int eagerRefreshWaitTimeMillis;
    private final OnEagerRefresh onEagerRefresh;
    private final int eagerRefreshBackoffMillis;
    private final int acquireConnectionBackoffMillis;

    public AcquireConnectionConfig(int maxWaitForConnection,
                                   int eagerRefreshWaitTimeMillis,
                                   OnEagerRefresh onEagerRefresh,
                                   int eagerRefreshBackoffMillis,
                                   int acquireConnectionBackoffMillis) {
        this.maxWaitForConnection = maxWaitForConnection;
        this.eagerRefreshWaitTimeMillis = eagerRefreshWaitTimeMillis;
        this.onEagerRefresh = onEagerRefresh;
        this.eagerRefreshBackoffMillis = eagerRefreshBackoffMillis;
        this.acquireConnectionBackoffMillis = acquireConnectionBackoffMillis;
    }

    public ConnectionAttemptManager createConnectionAttemptManager(GremlinClient gremlinClient){
        return new ConnectionAttemptManager(
                gremlinClient,
                maxWaitForConnection,
                eagerRefreshWaitTimeMillis,
                onEagerRefresh,
                eagerRefreshBackoffMillis);
    }

    public int acquireConnectionBackoffMillis() {
        return acquireConnectionBackoffMillis;
    }
}
