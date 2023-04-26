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

public interface EndpointFilter {

    EndpointFilter NULL_ENDPOINT_FILTER = new EndpointFilter() {
    };

    default ApprovalResult approveEndpoint(Endpoint endpoint) {
        return new ApprovalResult(true, null);
    }

    default Endpoint enrichEndpoint(Endpoint endpoint) {
        return endpoint;
    }
}
