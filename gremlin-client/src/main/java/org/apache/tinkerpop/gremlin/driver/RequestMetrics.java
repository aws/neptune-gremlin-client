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

public class RequestMetrics {
    private final long durationMillis;
    private final long totalRequests;
    private final int droppedRequests;
    private final int skippedResponses;
    private Collection<EndpointRequestMetrics> metrics;

    RequestMetrics(long durationMillis,
                   long totalRequests,
                   int droppedRequests,
                   int skippedResponses,
                   Collection<EndpointRequestMetrics> metrics) {
        this.durationMillis = durationMillis;
        this.totalRequests = totalRequests;
        this.droppedRequests = droppedRequests;
        this.skippedResponses = skippedResponses;
        this.metrics = metrics;
    }

    public long getDurationMillis() {
        return durationMillis;
    }

    public long getTotalRequests() {
        return totalRequests;
    }

    public int getDroppedRequests() {
        return droppedRequests;
    }

    public int getSkippedResponses() {
        return skippedResponses;
    }

    public Collection<EndpointRequestMetrics> getMetrics() {
        return metrics;
    }
}
