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

public class ApprovalResult {

    public static ApprovalResult APPROVED = new ApprovalResult(true, "");
    public static String REJECTED_REASON_ANNOTATION = "AWS:rejected_reason";
    private final boolean isApproved;
    private final String reason;

    public ApprovalResult(boolean isApproved, String reason) {
        this.isApproved = isApproved;
        this.reason = reason;
    }

    public boolean isApproved() {
        return isApproved;
    }

    public String reason() {
        return reason;
    }

    public Endpoint enrich(Endpoint endpoint){
        if (!isApproved){
            endpoint.setAnnotation(REJECTED_REASON_ANNOTATION, reason);
        }
        return endpoint;
    }

    @Override
    public String toString() {
        return "ApprovalResult{" +
                "isApproved=" + isApproved +
                ", reason='" + reason + '\'' +
                '}';
    }
}
