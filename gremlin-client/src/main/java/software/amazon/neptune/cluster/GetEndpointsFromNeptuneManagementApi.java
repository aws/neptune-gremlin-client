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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.neptune.NeptuneClient;
import software.amazon.awssdk.services.neptune.NeptuneClientBuilder;
import software.amazon.awssdk.services.neptune.model.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.EndpointCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.RegionUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class GetEndpointsFromNeptuneManagementApi implements ClusterEndpointsFetchStrategy, ClusterMetadataSupplier {

    private static final Logger logger = LoggerFactory.getLogger(GetEndpointsFromNeptuneManagementApi.class);

    private static final String ANNOTATION_KEY_PREFIX = "neptune:annotation:";

    private final ClusterEndpointsFetchStrategy innerStrategy;
    private final String clusterId;
    private final String region;
    private final String iamProfile;
    private final AwsCredentialsProvider credentials;
    private final AtomicReference<NeptuneClusterMetadata> cachedClusterMetadata = new AtomicReference<>();
    private final ClientOverrideConfiguration clientConfiguration;
    private SdkHttpClient.Builder<?> httpClientBuilder;

    GetEndpointsFromNeptuneManagementApi(String clusterId) {
        this(clusterId, RegionUtils.getCurrentRegionName());
    }


    GetEndpointsFromNeptuneManagementApi(String clusterId, String region) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE);
    }


    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, String iamProfile) {
        this(clusterId, region, iamProfile, null, null, null);
    }

    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, String iamProfile, ClientOverrideConfiguration clientConfiguration) {
        this(clusterId, region, iamProfile, null, clientConfiguration, null);
    }


    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, AwsCredentialsProvider credentials) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE, credentials, null, null);
    }

    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, AwsCredentialsProvider credentials, ClientOverrideConfiguration clientConfiguration) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE, credentials, clientConfiguration, null);
    }

    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, AwsCredentialsProvider credentials, ClientOverrideConfiguration clientConfiguration, SdkHttpClient.Builder<?> httpClientBuilder) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE, credentials, clientConfiguration, null);
    }



    private GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                 String region,
                                                 String iamProfile,
                                                 AwsCredentialsProvider credentials,
                                                 ClientOverrideConfiguration clientConfiguration,
                                                 SdkHttpClient.Builder<?> httpClientBuilder) {
        this.innerStrategy = new CommonClusterEndpointsFetchStrategy(this);
        this.clusterId = clusterId;
        this.region = region;
        this.iamProfile = iamProfile;
        this.credentials = credentials;
        this.clientConfiguration = clientConfiguration;
        this.httpClientBuilder = httpClientBuilder;
    }

    @Override
    public NeptuneClusterMetadata refreshClusterMetadata() {
        try {
            NeptuneClientBuilder builder = NeptuneClient.builder();

            if (clientConfiguration != null){
                builder = builder.overrideConfiguration(clientConfiguration);
            }
            if (httpClientBuilder != null) {
                builder = builder.httpClientBuilder(httpClientBuilder);
            }

            if (StringUtils.isNotEmpty(region)) {
                builder = builder.region(Region.of(region));
            }

            if (credentials != null) {
                builder = builder.credentialsProvider(credentials);
            } else if (!iamProfile.equals(IamAuthConfig.DEFAULT_PROFILE)) {
                builder = builder.credentialsProvider(ProfileCredentialsProvider.create(iamProfile));
            }

            NeptuneClient neptune = builder.build();

            DescribeDbClustersResponse describeDBClustersResult = neptune
                    .describeDBClusters(DescribeDbClustersRequest.builder().dbClusterIdentifier(clusterId).build());

            if (describeDBClustersResult.dbClusters().isEmpty()) {
                throw new IllegalStateException(String.format("Unable to find cluster %s", clusterId));
            }

            DBCluster dbCluster = describeDBClustersResult.dbClusters().get(0);

            String clusterEndpoint = dbCluster.endpoint();
            String readerEndpoint = dbCluster.readerEndpoint();

            List<DBClusterMember> dbClusterMembers = dbCluster.dbClusterMembers();
            Optional<DBClusterMember> clusterWriter = dbClusterMembers.stream()
                    .filter(DBClusterMember::isClusterWriter)
                    .findFirst();

            String primary = clusterWriter.map(DBClusterMember::dbInstanceIdentifier).orElse("");
            List<String> replicas = dbClusterMembers.stream()
                    .filter(dbClusterMember -> !dbClusterMember.isClusterWriter())
                    .map(DBClusterMember::dbInstanceIdentifier)
                    .collect(Collectors.toList());

            DescribeDbInstancesRequest describeDBInstancesRequest = DescribeDbInstancesRequest.builder()
                    .filters(
                            Collections.singletonList(
                                    Filter.builder()
                                            .name("db-cluster-id")
                                            .values(dbCluster.dbClusterIdentifier())
                                            .build()
                            )
                    )
                    .build();

            DescribeDbInstancesResponse describeDBInstancesResult = neptune
                    .describeDBInstances(describeDBInstancesRequest);

            Collection<NeptuneInstanceMetadata> instances = new ArrayList<>();
            describeDBInstancesResult.dbInstances()
                    .forEach(c -> {
                                String role = "unknown";
                                if (primary.equals(c.dbInstanceIdentifier())) {
                                    role = "writer";
                                }
                                if (replicas.contains(c.dbInstanceIdentifier())) {
                                    role = "reader";
                                }
                                String address = c.endpoint() == null ? null : c.endpoint().address();
                                Map<String, String> tags = getTags(c.dbInstanceArn(), neptune);
                                Map<String, String> annotations = getAnnotations(tags);
                                instances.add(
                                        new NeptuneInstanceMetadata()
                                                .withInstanceId(c.dbInstanceIdentifier())
                                                .withRole(role)
                                                .withAddress(address)
                                                .withStatus(c.dbInstanceStatus())
                                                .withAvailabilityZone(c.availabilityZone())
                                                .withInstanceType(c.dbInstanceClass())
                                                .withTags(tags)
                                                .withAnnotations(annotations));
                            }
                    );

            neptune.close();

            NeptuneClusterMetadata clusterMetadata = new NeptuneClusterMetadata()
                    .withInstances(instances)
                    .withClusterEndpoint(clusterEndpoint)
                    .withReaderEndpoint(readerEndpoint);

            cachedClusterMetadata.set(clusterMetadata);

            return clusterMetadata;

        } catch (NeptuneException e) {
            if (e.isThrottlingException()) {
                logger.warn("Calls to the Neptune Management API are being throttled. Reduce the refresh rate and stagger refresh agent requests, or use a NeptuneEndpointsInfoLambda proxy.");
                NeptuneClusterMetadata clusterMetadata = cachedClusterMetadata.get();
                if (clusterMetadata != null) {
                    return clusterMetadata;
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }

    }



    @Override
    public NeptuneClusterMetadata getClusterMetadata() {
        NeptuneClusterMetadata clusterMetadata = cachedClusterMetadata.get();
        if (clusterMetadata == null) {
            return refreshClusterMetadata();
        }
        return clusterMetadata;
    }

    @Override
    public ClusterMetadataSupplier clusterMetadataSupplier() {
        return this;
    }

    @Override
    public Map<? extends EndpointsSelector, EndpointCollection> getEndpoints(Collection<? extends EndpointsSelector> selectors, boolean refresh) {
        return innerStrategy.getEndpoints(selectors, refresh);
    }

    private Map<String, String> getTags(String dbInstanceArn, NeptuneClient neptune) {

        List<Tag> tagList = neptune.listTagsForResource(
                ListTagsForResourceRequest.builder()
                        .resourceName(dbInstanceArn)
                        .build()).tagList();

        Map<String, String> tags = new HashMap<>();
        tagList.forEach(t -> tags.put(t.key(), t.value()));

        return tags;
    }

    private Map<String, String> getAnnotations(Map<String, String> tags) {
        Map<String, String> annotations = new HashMap<>();

        for (Map.Entry<String, String> tag : tags.entrySet()) {
            String key = tag.getKey();
            if (key.startsWith(ANNOTATION_KEY_PREFIX)){
                annotations.put(key.substring(ANNOTATION_KEY_PREFIX.length()), tag.getValue());
            }
        }

        return annotations;
    }
}
