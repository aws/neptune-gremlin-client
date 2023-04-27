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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class NeptuneInstanceMetadataTest {

    @Test
    public void shouldSerializeAndDeserialize() throws IOException {

        HashMap<String, String> tags = new HashMap<>();
        tags.put("name", "my-writer");
        tags.put("app", "analytics");

        NeptuneInstanceMetadata instance = new NeptuneInstanceMetadata()
                .withInstanceId("instance-2")
                .withInstanceType("r5.medium")
                .withAvailabilityZone("eu-west-1a")
                .withAddress("endpoint-2")
                .withStatus("rebooting")
                .withRole("reader")
                .withTags(tags)
                .withAnnotation("annotation-2-key", "annotation-2-value");

        String json1 = instance.toJsonString();
        NeptuneInstanceMetadata i = NeptuneInstanceMetadata.fromByteArray(json1.getBytes());
        String json2 = i.toJsonString();

        Assert.assertEquals(json2, json1);


    }

    @Test
    public void shouldAcceptEndpointFieldForAddressValue() throws IOException {
        String json = "{\"instanceId\":\"instance-2\",\"role\":\"reader\",\"endpoint\":\"endpoint-2\",\"status\":\"rebooting\",\"availabilityZone\":\"eu-west-1a\",\"instanceType\":\"r5.medium\",\"annotations\":{\"annotation-2-key\":\"annotation-2-value\"},\"tags\":{\"app\":\"analytics\",\"name\":\"my-writer\"}}\n";
        NeptuneInstanceMetadata i = NeptuneInstanceMetadata.fromByteArray(json.getBytes());

        assertEquals("endpoint-2", i.getAddress());
    }

}