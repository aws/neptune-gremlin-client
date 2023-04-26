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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class ClusterContext implements AutoCloseable {

    private final GremlinCluster cluster;
    private final GremlinClient client;
    private final GraphTraversalSource graphTraversalSource;

    public ClusterContext(GremlinCluster cluster,
                          GremlinClient client,
                          GraphTraversalSource graphTraversalSource) {
        this.cluster = cluster;
        this.client = client;
        this.graphTraversalSource = graphTraversalSource;
    }

    public GraphTraversalSource graphTraversalSource() {
        return graphTraversalSource;
    }

    public GremlinCluster cluster() {
        return cluster;
    }

    public GremlinClient client() {
        return client;
    }

    @Override
    public void close() throws Exception {
        client.close();
        cluster.close();
    }
}
