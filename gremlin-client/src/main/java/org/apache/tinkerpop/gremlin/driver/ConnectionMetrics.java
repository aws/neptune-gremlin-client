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

import java.util.Collection;

public class ConnectionMetrics {

    private final long durationMillis;
    private final long totalConnectionAttempts;
    private final Collection<EndpointConnectionMetrics> metrics;

    ConnectionMetrics(long durationMillis, long totalConnectionAttempts, Collection<EndpointConnectionMetrics> metrics) {
        this.durationMillis = durationMillis;
        this.totalConnectionAttempts = totalConnectionAttempts;
        this.metrics = metrics;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public long getTotalConnectionAttempts() {
        return totalConnectionAttempts;
    }

    public Collection<EndpointConnectionMetrics> getMetrics() {
        return metrics;
    }
}
