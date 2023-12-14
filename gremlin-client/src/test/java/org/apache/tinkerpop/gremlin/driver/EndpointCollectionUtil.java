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

package org.apache.tinkerpop.gremlin.driver;

public class EndpointCollectionUtil {

    public static EndpointCollection getEndpointsWithNoCluster(EndpointCollection c, ClientClusterCollection clientClusterCollection) {
        return c.getEndpointsWithNoCluster(clientClusterCollection);
    }

    public static EndpointCollection getEnrichedEndpoints(EndpointCollection c, EndpointFilter endpointFilter) {
        return c.getEnrichedEndpoints(endpointFilter);
    }

    public static EndpointCollection getAcceptedEndpoints(EndpointCollection c, EndpointFilter endpointFilter) {
        return c.getAcceptedEndpoints(endpointFilter);
    }

    public static EndpointCollection getRejectedEndpoints(EndpointCollection c, EndpointFilter endpointFilter) {
        return c.getRejectedEndpoints(endpointFilter);
    }
}
