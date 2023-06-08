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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.gremlin.driver.Endpoint;

import java.io.IOException;
import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NeptuneInstanceMetadata implements Endpoint {

    public static NeptuneInstanceMetadata fromByteArray(byte[] bytes) throws IOException {
        return new ObjectMapper().readerFor(NeptuneInstanceMetadata.class).readValue(bytes);
    }

    private static final Collection<String> AVAILABLE_STATES = Arrays.asList("available", "backing-up", "modifying", "upgrading");
    private String instanceId;
    private String role;
    private String address;
    private String status;
    private String availabilityZone;
    private String instanceType;

    private final Map<String, String> annotations = new HashMap<>();
    private final Map<String, String> tags = new HashMap<>();

    private final Map<String, Double> metrics = new HashMap<>();

    public NeptuneInstanceMetadata() {

    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Deprecated
    public void setEndpoint(String endpoint) {
        this.address = endpoint;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setAvailabilityZone(String availabilityZone) {
        this.availabilityZone = availabilityZone;
    }

    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    public void setTags(Map<String, String> tags) {
        this.tags.clear();
        this.tags.putAll(tags);
    }

    public void setAnnotations(Map<String, String> annotations) {
        this.annotations.clear();
        this.annotations.putAll(annotations);
    }

    public void setMetrics(Map<String, Double> metrics) {
        this.metrics.clear();
        this.metrics.putAll(metrics);
    }

    @Override
    public void setAnnotation(String key, String value){
        annotations.put(key, value);
    }

    @Override
    public void setMetric(String key, Double value){
        metrics.put(key, value);
    }

    public NeptuneInstanceMetadata withInstanceId(String instanceId) {
        setInstanceId(instanceId);
        return this;
    }

    public NeptuneInstanceMetadata withRole(String role) {
        setRole(role);
        return this;
    }

    public NeptuneInstanceMetadata withAddress(String address) {
        setAddress(address);
        return this;
    }

    public NeptuneInstanceMetadata withStatus(String status) {
        setStatus(status);
        return this;
    }

    public NeptuneInstanceMetadata withAvailabilityZone(String availabilityZone) {
        setAvailabilityZone(availabilityZone);
        return this;
    }

    public NeptuneInstanceMetadata withInstanceType(String instanceType) {
        setInstanceType(instanceType);
        return this;
    }

    public NeptuneInstanceMetadata withTags(Map<String, String> tags) {
        setTags(tags);
        return this;
    }

    public NeptuneInstanceMetadata withAnnotations(Map<String, String> annotations) {
        setAnnotations(annotations);
        return this;
    }

    public NeptuneInstanceMetadata withAnnotation(String key, String value) {
        annotations.put(key, value);
        return this;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getRole() {
        return role;
    }

    @Override
    public String getAddress() {
        return address;
    }

    public String getStatus() {
        return status;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public Map<String, String> getAnnotations() {
        return annotations;
    }

    @Override
    public Map<String, Double> getMetrics() {
        return metrics;
    }

    public boolean hasTag(String tag) {
        return tags.containsKey(tag);
    }

    public String getTag(String tag) {
        return tags.get(tag);
    }

    public String getTag(String tag, String defaultValue) {
        if (!tags.containsKey(tag)) {
            return defaultValue;
        }
        return tags.get(tag);
    }
    public boolean hasTag(String tag, String value) {
        return hasTag(tag) && getTag(tag).equals(value);
    }

    @JsonIgnore
    public boolean isAvailable() {
        return address != null && AVAILABLE_STATES.contains(getStatus().toLowerCase());
    }
    @JsonIgnore
    public boolean isPrimary() {
        return getRole().equalsIgnoreCase("writer");
    }

    @JsonIgnore
    public boolean isReader() {
        return getRole().equalsIgnoreCase("reader");
    }

    @Override
    public String toString() {
        return "NeptuneEndpointMetadata{" +
                "instanceId='" + instanceId + '\'' +
                ", role='" + role + '\'' +
                ", address='" + address + '\'' +
                ", status='" + status + '\'' +
                ", availabilityZone='" + availabilityZone + '\'' +
                ", instanceType='" + instanceType + '\'' +
                ", annotations=" + annotations +
                ", tags=" + tags +
                ", metrics=" + metrics +
                "}";
    }

    public String toJsonString() throws JsonProcessingException {
        return new ObjectMapper().writerFor(this.getClass()).writeValueAsString(this);
    }
}
