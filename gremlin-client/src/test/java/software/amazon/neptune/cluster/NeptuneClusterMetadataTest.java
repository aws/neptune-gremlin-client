/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package software.amazon.neptune.cluster;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

public class NeptuneClusterMetadataTest {

    @Test
    public void serializeAndDeserializeClusterMetadata() throws IOException {
        HashMap<String, String> tags = new HashMap<>();
        tags.put("name", "my-writer");
        tags.put("app", "analytics");

        NeptuneInstanceMetadata instance1 = new NeptuneInstanceMetadata()
                .withInstanceId("instance-1")
                .withInstanceType("r5.large")
                .withAvailabilityZone("eu-west-1b")
                .withAddress("endpoint-1")
                .withStatus("available")
                .withRole("writer")
                .withTags(tags)
                .withAnnotation("annotation-1-key", "annotation-1-value");

        NeptuneInstanceMetadata instance2 = new NeptuneInstanceMetadata()
                .withInstanceId("instance-2")
                .withInstanceType("r5.medium")
                .withAvailabilityZone("eu-west-1a")
                .withAddress("endpoint-2")
                .withStatus("rebooting")
                .withRole("reader")
                .withTags(tags)
                .withAnnotation("annotation-2-key", "annotation-2-value");

        NeptuneClusterMetadata neptuneClusterMetadata = new NeptuneClusterMetadata()
                .withClusterEndpoint("cluster-endpoint")
                .withReaderEndpoint("reader-endpoint")
                .withInstances(Arrays.asList(instance1, instance2));

        String json1 = neptuneClusterMetadata.toJsonString();
        NeptuneClusterMetadata cluster = NeptuneClusterMetadata.fromByteArray(json1.getBytes());
        String json2 = cluster.toJsonString();

        assertEquals(json2, json1);
    }

    @Test
    public void shouldAcceptEndpointFieldForAddressValue() throws IOException {
        String json = "{\n" +
                "  \"instances\": [\n" +
                "    {\n" +
                "      \"instanceId\": \"neptune-db-1-123456b0\",\n" +
                "      \"role\": \"writer\",\n" +
                "      \"endpoint\": \"neptune-db-1-123456b0.abcdefghijklm.eu-west-2.neptune.amazonaws.com\",\n" +
                "      \"status\": \"available\",\n" +
                "      \"availabilityZone\": \"eu-west-2a\",\n" +
                "      \"instanceType\": \"db.r5.large\",\n" +
                "      \"annotations\": {},\n" +
                "      \"tags\": {\n" +
                "        \"Name\": \"neptune-db-1-123456b0\",\n" +
                "        \"Stack\": \"eu-west-2-social-NeptuneBaseStack-ABCDEFGHIJKL\",\n" +
                "        \"StackId\": \"arn:aws:cloudformation:eu-west-2:123456789123:stack/social-NeptuneBaseStack-ABCDEFGHIJKL/828f4fe0-e4e4-11ed-9c0c-02f21623886a\"\n" +
                "      }\n" +
                "    },\n" +
                "    {\n" +
                "      \"instanceId\": \"neptune-db-2-123456b0\",\n" +
                "      \"role\": \"reader\",\n" +
                "      \"endpoint\": \"neptune-db-2-123456b0.abcdefghijklm.eu-west-2.neptune.amazonaws.com\",\n" +
                "      \"status\": \"available\",\n" +
                "      \"availabilityZone\": \"eu-west-2c\",\n" +
                "      \"instanceType\": \"db.r5.large\",\n" +
                "      \"annotations\": {},\n" +
                "      \"tags\": {\n" +
                "        \"Name\": \"neptune-db-2-123456b0\",\n" +
                "        \"Stack\": \"eu-west-2-social-NeptuneBaseStack-ABCDEFGHIJKL\",\n" +
                "        \"StackId\": \"arn:aws:cloudformation:eu-west-2:123456789123:stack/social-NeptuneBaseStack-ABCDEFGHIJKL/828f4fe0-e4e4-11ed-9c0c-02f21623886a\"\n" +
                "      }\n" +
                "    }\n" +
                "  ],\n" +
                "  \"clusterEndpoint\": {\n" +
                "    \"endpoint\": \"neptune-cluster-123456b0.cluster-abcdefghijklm.eu-west-2.neptune.amazonaws.com\",\n" +
                "    \"annotations\": {}\n" +
                "  },\n" +
                "  \"readerEndpoint\": {\n" +
                "    \"endpoint\": \"neptune-cluster-123456b0.cluster-ro-abcdefghijklm.eu-west-2.neptune.amazonaws.com\",\n" +
                "    \"annotations\": {}\n" +
                "  }\n" +
                "}\n";
        NeptuneClusterMetadata cluster = NeptuneClusterMetadata.fromByteArray(json.getBytes());
        String address = cluster.getInstances().stream().filter(i -> i.isPrimary()).map(i -> i.getAddress()).findFirst().get();
        assertEquals("neptune-db-1-123456b0.abcdefghijklm.eu-west-2.neptune.amazonaws.com", address);
    }
}
