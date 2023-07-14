/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package software.amazon.neptune.cluster;

import org.apache.tinkerpop.gremlin.driver.EndpointCollection;
import org.apache.tinkerpop.gremlin.driver.GremlinClient;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

class GetCurrentEndpointsFromGremlinClient implements ClusterEndpointsFetchStrategy {
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
