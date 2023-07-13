package org.apache.tinkerpop.gremlin.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class RequestMetrics {

    public final Map<String, EndpointRequestMetrics> metrics;
    public final Map<UUID, String> traceIds = new HashMap<>();

    public RequestMetrics(Map<String, EndpointRequestMetrics> metrics) {
        this.metrics = metrics;
    }

    public void registerAddressForTraceId(String address, UUID traceId) {
        traceIds.put(traceId, address);
    }

    public void registerDurationForTraceId(UUID traceId, long durationMillis) {
        String address = traceIds.remove(traceId);
        if (address != null) {
            if (!metrics.containsKey(address)) {
                metrics.put(address, new EndpointRequestMetrics(address));
            }
            metrics.get(address).update(durationMillis);
        }
    }

    public Map<String, EndpointRequestMetrics> endpointRequestMetrics() {
        return metrics;
    }

}
