package org.apache.tinkerpop.gremlin.driver;

class MetricsConfig {
    private final boolean enableMetrics;
    private final MetricsHandlerCollection metricsHandlers;

    MetricsConfig(boolean enableMetrics, MetricsHandlerCollection metricsHandlers) {
        this.enableMetrics = enableMetrics;
        this.metricsHandlers = metricsHandlers;
    }

    public boolean enableMetrics() {
        return enableMetrics;
    }

    public MetricsHandlerCollection metricsHandlers() {
        return metricsHandlers;
    }
}
