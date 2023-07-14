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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.driver.DatabaseEndpoint;
import org.apache.tinkerpop.gremlin.driver.EndpointCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class NeptuneClusterMetadata {

    public static NeptuneClusterMetadata fromByteArray(byte[] bytes) throws IOException {
        return new ObjectMapper().readerFor(NeptuneClusterMetadata.class).readValue(bytes);
    }

    private final Collection<NeptuneInstanceMetadata> instances = new ArrayList<>();
    private DatabaseEndpoint clusterEndpoint;
    private DatabaseEndpoint readerEndpoint;

    public NeptuneClusterMetadata(){

    }

    public void setClusterEndpoint(DatabaseEndpoint clusterEndpoint) {
        this.clusterEndpoint = clusterEndpoint;
    }

    public void setReaderEndpoint(DatabaseEndpoint readerEndpoint) {
        this.readerEndpoint = readerEndpoint;
    }

    public void setInstances(Collection<NeptuneInstanceMetadata> instances) {
        this.instances.clear();
        this.instances.addAll(instances);
    }

    public NeptuneClusterMetadata withClusterEndpoint(String clusterEndpoint) {
        setClusterEndpoint(new DatabaseEndpoint().withAddress(clusterEndpoint));
        return this;
    }

    public NeptuneClusterMetadata withReaderEndpoint(String readerEndpoint) {
        setReaderEndpoint(new DatabaseEndpoint().withAddress(readerEndpoint));
        return this;
    }

    public NeptuneClusterMetadata withInstances(Collection<NeptuneInstanceMetadata> instances) {
        setInstances(instances);
        return this;
    }

    public Collection<NeptuneInstanceMetadata> getInstances() {
        return instances;
    }

    public DatabaseEndpoint getClusterEndpoint() {
        return clusterEndpoint;
    }

    public DatabaseEndpoint getReaderEndpoint() {
        return readerEndpoint;
    }

    public EndpointCollection select(EndpointsSelector selector){
        return selector.getEndpoints(this);
    }

    @Override
    public String toString() {
        return "NeptuneClusterMetadata{" +
                "instances=" + instances +
                ", clusterEndpoint='" + clusterEndpoint + '\'' +
                ", readerEndpoint='" + readerEndpoint + '\'' +
                '}';
    }

    public String toJsonString() throws JsonProcessingException {
        return new ObjectMapper().writerFor(this.getClass()).writeValueAsString(this);
    }
}
