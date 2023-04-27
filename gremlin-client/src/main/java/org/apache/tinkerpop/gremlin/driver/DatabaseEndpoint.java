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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.HashMap;
import java.util.Map;

public class DatabaseEndpoint implements Endpoint {

    private String address;
    private final Map<String, String> annotations = new HashMap<>();

    public void setAddress(String address) {
        this.address = address;
    }

    @Deprecated
    public void setEndpoint(String endpoint) {
        this.address = endpoint;
    }

    public void setAnnotations(Map<String, String> annotations) {
        this.annotations.clear();
        this.annotations.putAll(annotations);
    }

    public DatabaseEndpoint withAddress(String endpoint) {
        setAddress(endpoint);
        return this;
    }

    public DatabaseEndpoint withAnnotations(Map<String, String> annotations) {
        setAnnotations(annotations);
        return this;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    @JsonIgnore
    public boolean isAvailable() {
        return true;
    }

    @Override
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    @Override
    public void setAnnotation(String key, String value) {
        annotations.put(key, value);
    }

    @Override
    public String toString() {
        return "DatabaseEndpoint{" +
                "address='" + address + '\'' +
                ", annotations=" + annotations +
                '}';
    }
}
