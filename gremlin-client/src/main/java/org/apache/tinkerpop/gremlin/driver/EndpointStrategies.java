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

class EndpointStrategies {
    private final EndpointFilter endpointFilter;
    private final EndpointSelectionStrategy endpointSelectionStrategy;

    EndpointStrategies(EndpointFilter endpointFilter, EndpointSelectionStrategy endpointSelectionStrategy) {
        this.endpointFilter = endpointFilter;
        this.endpointSelectionStrategy = endpointSelectionStrategy;
    }

    public EndpointFilter endpointFilter() {
        return endpointFilter;
    }

    public EndpointSelectionStrategy endpointSelectionStrategy() {
        return endpointSelectionStrategy;
    }

    @Override
    public String toString() {
        return "EndpointStrategies{" +
                "endpointFilter=" + endpointFilter.getClass().getSimpleName() +
                ", endpointSelectionStrategy=" + endpointSelectionStrategy.getClass().getSimpleName() +
                '}';
    }
}
