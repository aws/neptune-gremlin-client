package org.apache.tinkerpop.gremlin.driver;

import java.util.Collection;
import java.util.Map;

public class ConnectionMetricsCollector {
    private final Map<String, EndpointConnectionMetrics> connectionMetrics;

    public ConnectionMetricsCollector(Map<String, EndpointConnectionMetrics> connectionMetrics) {
        this.connectionMetrics = connectionMetrics;
    }

    public Collection<EndpointConnectionMetrics> metrics(){
        return connectionMetrics.values();
    }

    public long totalConnectionAttempts(){
        long totalConnectionAttempts = 0;
        for (EndpointConnectionMetrics cm : connectionMetrics.values()) {
            totalConnectionAttempts += cm.getTotalAttempts();
        }
        return totalConnectionAttempts;
    }

    void succeeded(String address, long startMillis){
        if (connectionMetrics.containsKey(address)){
            connectionMetrics.get(address).succeeded(startMillis);
        }
    }

    void unavailable(String address, long startMillis){
        if (connectionMetrics.containsKey(address)) {
            connectionMetrics.get(address).unavailable(startMillis);
        }
    }

    void closing(String address, long startMillis){
        if (connectionMetrics.containsKey(address)) {
            connectionMetrics.get(address).closing(startMillis);
        }
    }

    void dead(String address, long startMillis){
        if (connectionMetrics.containsKey(address)) {
            connectionMetrics.get(address).dead(startMillis);
        }
    }

    void npe(String address, long startMillis){
        if (connectionMetrics.containsKey(address)) {
            connectionMetrics.get(address).npe(startMillis);
        }
    }

    void nha(String address, long startMillis){
        if (connectionMetrics.containsKey(address)) {
            connectionMetrics.get(address).nha(startMillis);
        }
    }


}
