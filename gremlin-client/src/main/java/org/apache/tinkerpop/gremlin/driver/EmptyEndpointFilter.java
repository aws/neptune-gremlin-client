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

public class EmptyEndpointFilter implements AvailableEndpointFilter {
    private final AvailableEndpointFilter innerFilter;

    public EmptyEndpointFilter(AvailableEndpointFilter innerFilter) {
        this.innerFilter = innerFilter != null ? innerFilter : AvailableEndpointFilter.NULL_ENDPOINT_FILTER;
    }

    @Override
    public ApprovalResult approveEndpoint(Endpoint endpoint) {
        if (endpoint.getAddress() == null) {
            return new ApprovalResult(false, "empty");
        } else {
            return innerFilter.approveEndpoint(endpoint);
        }
    }

    @Override
    public Endpoint enrichEndpoint(Endpoint endpoint) {
        if (endpoint.getAddress() == null) {
            return endpoint;
        } else {
            return innerFilter.enrichEndpoint(endpoint);
        }
    }
}
