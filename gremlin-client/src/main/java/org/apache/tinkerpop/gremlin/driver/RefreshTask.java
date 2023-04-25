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

import software.amazon.neptune.cluster.EndpointsSelector;

public class RefreshTask {
    public static <T extends EndpointsSelector> RefreshTask refresh(GremlinClient client, T selector){
        return new RefreshTask(client, selector);
    }

    private final GremlinClient client;
    private final EndpointsSelector selector;

    public <T extends EndpointsSelector> RefreshTask(GremlinClient client, T selector) {
        this.client = client;
        this.selector = selector;
    }

    public GremlinClient client() {
        return client;
    }

    public EndpointsSelector selector() {
        return selector;
    }
}
