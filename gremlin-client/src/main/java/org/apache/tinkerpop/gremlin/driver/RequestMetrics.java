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
