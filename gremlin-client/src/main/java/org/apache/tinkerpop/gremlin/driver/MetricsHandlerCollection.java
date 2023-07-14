package org.apache.tinkerpop.gremlin.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

class MetricsHandlerCollection implements MetricsHandler{

    private static final Logger logger = LoggerFactory.getLogger(MetricsLogger.class);

    private final Collection<MetricsHandler> handlers = new ArrayList<>();

    MetricsHandlerCollection(){
        addHandler(new MetricsLogger());
    }

    void addHandler(MetricsHandler handler){
        handlers.add(handler);
    }

    @Override
    public void onMetricsPublished(ConnectionMetrics connectionMetrics, RequestMetrics requestMetrics) {
        for (MetricsHandler handler : handlers) {
            try{
                handler.onMetricsPublished(connectionMetrics, requestMetrics);
            } catch (Exception e){
                logger.error("Error while handling metrics", e);
            }
        }
    }
}
