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

import org.apache.tinkerpop.gremlin.driver.ApprovalResult;
import org.apache.tinkerpop.gremlin.driver.EndpointFilter;
import org.apache.tinkerpop.gremlin.driver.Endpoint;

import java.util.Map;

public class SuspendedEndpoints implements EndpointFilter {
    public static final String STATE_ANNOTATION = "AWS:endpoint_state";
    public static final String SUSPENDED = "suspended";

    @Override
    public ApprovalResult approveEndpoint(Endpoint endpoint) {
        Map<String, String> annotations = endpoint.getAnnotations();
        if (annotations.containsKey(STATE_ANNOTATION) && annotations.get(STATE_ANNOTATION).equals(SUSPENDED)){
            return new ApprovalResult(false, SUSPENDED);
        } else {
            return ApprovalResult.APPROVED;
        }
    }
}
