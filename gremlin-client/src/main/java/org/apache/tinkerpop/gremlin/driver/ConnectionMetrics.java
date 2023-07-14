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
