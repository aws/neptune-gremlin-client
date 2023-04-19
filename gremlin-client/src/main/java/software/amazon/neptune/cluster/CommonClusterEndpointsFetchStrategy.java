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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CommonClusterEndpointsFetchStrategy implements ClusterEndpointsFetchStrategy {

    private final ClusterMetadataSupplier clusterMetadataSupplier;

    public CommonClusterEndpointsFetchStrategy(ClusterMetadataSupplier clusterMetadataSupplier) {
        this.clusterMetadataSupplier = clusterMetadataSupplier;
    }

    @Override
    public ClusterMetadataSupplier clusterMetadataSupplier() {
        return clusterMetadataSupplier;
    }

    @Override
    public Map<? extends EndpointsSelector, EndpointCollection> getEndpoints(Collection<? extends EndpointsSelector> selectors, boolean refresh) {
        if (refresh) {
            return refreshEndpoints(selectors);
        }

        NeptuneClusterMetadata clusterMetadata = clusterMetadataSupplier().getClusterMetadata();

        if (clusterMetadata == null) {
            return refreshEndpoints(selectors);
        }

        Map<EndpointsSelector, EndpointCollection> results = new HashMap<>();

        for (EndpointsSelector selector : selectors) {
            results.put(selector, clusterMetadata.select(selector));
        }

        return results;
    }

    private Map<? extends EndpointsSelector, EndpointCollection> refreshEndpoints(Collection<? extends EndpointsSelector> selectors) {
        NeptuneClusterMetadata clusterMetadata = clusterMetadataSupplier.refreshClusterMetadata();

        Map<EndpointsSelector, EndpointCollection> results = new HashMap<>();

        for (EndpointsSelector selector : selectors) {
            results.put(selector, clusterMetadata.select(selector));
        }

        return results;
    }
}
