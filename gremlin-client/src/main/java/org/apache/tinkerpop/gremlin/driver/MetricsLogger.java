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

        logger.info("Request metrics: [duration: {}ms, totalRequests:{}, endpoints: [{}] (dropped: {}, skipped: {})]",
                requestMetrics.getDurationMillis(),
                requestMetrics.getTotalRequests(),

                requestMetrics.getMetrics().stream()
                        .map(EndpointRequestMetrics::toString)
                        .collect(Collectors.joining(", ")),
                requestMetrics.getDroppedRequests(),
                requestMetrics.getSkippedResponses());
    }
}
