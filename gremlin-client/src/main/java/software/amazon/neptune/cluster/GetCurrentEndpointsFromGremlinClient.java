package software.amazon.neptune.cluster;

import org.apache.tinkerpop.gremlin.driver.EndpointCollection;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class GetCurrentEndpointsFromGremlinClient implements ClusterEndpointsFetchStrategy {
    @Override
    public ClusterMetadataSupplier clusterMetadataSupplier() {
        throw new UnsupportedOperationException("This operation is not supported for the GetCurrentEndpointsFromGremlinClient strategy.");
    }

    @Override
    public Map<? extends EndpointsSelector, EndpointCollection> getEndpoints(Collection<? extends EndpointsSelector> selectors, boolean refresh) {
        throw new UnsupportedOperationException("This operation is not supported for the GetCurrentEndpointsFromGremlinClient strategy.");
    }

    @Override
    public Map<? extends EndpointsSelector, EndpointCollection> getEndpoints(Map<? extends EndpointsSelector, GremlinClient> clientSelectors, boolean refresh) {
        Map<EndpointsSelector, EndpointCollection> results = new HashMap<>();
        for (Map.Entry<? extends EndpointsSelector, GremlinClient> clientSelector : clientSelectors.entrySet()) {
            results.put(clientSelector.getKey(), clientSelector.getValue().currentEndpoints());
        }
        return results;
    }
}
