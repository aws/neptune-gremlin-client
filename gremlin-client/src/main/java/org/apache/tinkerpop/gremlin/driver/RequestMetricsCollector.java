package org.apache.tinkerpop.gremlin.driver;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

class RequestMetricsCollector {

    private static final int MAX_NUMBER_TRACE_IDS = 9000;

    public final Map<String, EndpointRequestMetrics> metrics;
    public final Map<UUID, String> traceIds = new LinkedHashMap<>(16, 0.75f, true);

    private int dropped = 0;

    private int skipped = 0;

    public RequestMetricsCollector(Map<String, EndpointRequestMetrics> metrics) {
        this.metrics = metrics;
    }

    public void registerAddressForTraceId(UUID traceId, String address) {
        if (traceIds.size() > MAX_NUMBER_TRACE_IDS){
            UUID toRemove = traceIds.keySet().iterator().next();
            traceIds.remove(toRemove);
            dropped++;
        }
        traceIds.put(traceId, address);
    }

    public void registerDurationForTraceId(UUID traceId, long durationMillis) {
        String address = traceIds.remove(traceId);
        if (address != null) {
            if (metrics.containsKey(address)) {
                metrics.get(address).update(durationMillis);
            } else {
                skipped++;
            }
        } else {
            skipped++;
        }
    }

    public int droppedRequests(){
        return dropped;
    }

    public int skippedResponses(){
        return skipped;
    }
    public long totalRequests(){
        long totalRequests = 0;
        for (EndpointRequestMetrics rm : metrics.values()) {
            totalRequests += rm.getCount();
        }
        return totalRequests;
    }

    public Collection<EndpointRequestMetrics> metrics(){
        return metrics.values();
    }

}
