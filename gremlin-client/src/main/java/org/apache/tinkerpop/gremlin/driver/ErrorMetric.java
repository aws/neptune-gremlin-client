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

public class ErrorMetric {
    private final Class<? extends Throwable> errorClass;
    private long count;

    public ErrorMetric(Class<? extends Throwable> errorClass) {
        this.errorClass = errorClass;
    }

    public Class<? extends Throwable>  getErrorClass(){
        return errorClass;
    }

    public long getCount() {
        return count;
    }

    public ErrorMetric increment(){
        count++;
        return this;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", errorClass.getSimpleName(), count);
    }
}
