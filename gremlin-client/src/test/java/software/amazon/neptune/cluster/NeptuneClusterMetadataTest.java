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

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;

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
                .withEndpoint("endpoint-1")
                .withStatus("available")
                .withRole("writer")
                .withTags(tags)
                .withAnnotation("annotation-1-key", "annotation-1-value");

        NeptuneInstanceMetadata instance2 = new NeptuneInstanceMetadata()
                .withInstanceId("instance-2")
                .withInstanceType("r5.medium")
                .withAvailabilityZone("eu-west-1a")
                .withEndpoint("endpoint-2")
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

        Assert.assertEquals(json2, json1);
    }
}
