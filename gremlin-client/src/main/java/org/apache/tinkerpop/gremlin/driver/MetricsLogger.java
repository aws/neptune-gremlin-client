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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

class MetricsLogger implements MetricsHandler {

    private static final Logger logger = LoggerFactory.getLogger(MetricsLogger.class);

    @Override
    public void onMetricsPublished(ConnectionMetrics connectionMetrics, RequestMetrics requestMetrics) {
        logger.info("Connection metrics: [duration: {}ms, totalConnectionAttempts:{}, endpoints: [{}]]",
                connectionMetrics.getDurationMillis(),
                connectionMetrics.getTotalConnectionAttempts(),
                connectionMetrics.getMetrics().stream()
                        .map(EndpointConnectionMetrics::toString)
                        .collect(Collectors.joining(", ")));

        logger.info("Request metrics: [duration: {}ms, totalRequests:{}, failed: {}, endpoints: [{}] (dropped: {}, skipped: {})]",
                requestMetrics.getDurationMillis(),
                requestMetrics.getTotalRequests(),
                requestMetrics.getFailedRequestsCount(),
                requestMetrics.getMetrics().stream()
                        .map(EndpointRequestMetrics::toString)
                        .collect(Collectors.joining(", ")),
                requestMetrics.getDroppedRequestsCount(),
                requestMetrics.getSkippedResponsesCount());
    }
}
