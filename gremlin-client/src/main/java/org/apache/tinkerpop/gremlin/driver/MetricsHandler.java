package org.apache.tinkerpop.gremlin.driver;

public interface MetricsHandler {
    void onMetricsPublished(ConnectionMetrics connectionMetrics, RequestMetrics requestMetrics);
}
