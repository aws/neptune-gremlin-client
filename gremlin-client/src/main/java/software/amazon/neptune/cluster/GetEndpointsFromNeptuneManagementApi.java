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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.AmazonNeptuneClientBuilder;
import com.amazonaws.services.neptune.model.*;
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

    private final ClusterEndpointsFetchStrategy innerStrategy;
    private static final Map<String, Map<String, String>> instanceTags = new HashMap<>();
    private final String clusterId;
    private final String region;
    private final String iamProfile;
    private final AWSCredentialsProvider credentials;
    private final AtomicReference<NeptuneClusterMetadata> cachedClusterMetadata = new AtomicReference<>();

    private final ClientConfiguration clientConfiguration;

    GetEndpointsFromNeptuneManagementApi(String clusterId) {
        this(clusterId, RegionUtils.getCurrentRegionName());
    }


    GetEndpointsFromNeptuneManagementApi(String clusterId, String region) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE);
    }


    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, String iamProfile) {
        this(clusterId, region, iamProfile, null, null);
    }

    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, String iamProfile, ClientConfiguration clientConfiguration) {
        this(clusterId, region, iamProfile, null, clientConfiguration);
    }


    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, AWSCredentialsProvider credentials) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE, credentials, null);
    }

    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, AWSCredentialsProvider credentials, ClientConfiguration clientConfiguration) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE, credentials, clientConfiguration);
    }


    private GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                 String region,
                                                 String iamProfile,
                                                 AWSCredentialsProvider credentials,
                                                 ClientConfiguration clientConfiguration) {
        this.innerStrategy = new CommonClusterEndpointsFetchStrategy(this);
        this.clusterId = clusterId;
        this.region = region;
        this.iamProfile = iamProfile;
        this.credentials = credentials;
        this.clientConfiguration = clientConfiguration;
    }

    @Override
    public NeptuneClusterMetadata refreshClusterMetadata() {
        try {
            AmazonNeptuneClientBuilder builder = AmazonNeptuneClientBuilder.standard();

            if (clientConfiguration != null){
                builder = builder.withClientConfiguration(clientConfiguration);
            }

            if (StringUtils.isNotEmpty(region)) {
                builder = builder.withRegion(region);
            }

            if (credentials != null) {
                builder = builder.withCredentials(credentials);
            } else if (!iamProfile.equals(IamAuthConfig.DEFAULT_PROFILE)) {
                builder = builder.withCredentials(new ProfileCredentialsProvider(iamProfile));
            }

            AmazonNeptune neptune = builder.build();

            DescribeDBClustersResult describeDBClustersResult = neptune
                    .describeDBClusters(new DescribeDBClustersRequest().withDBClusterIdentifier(clusterId));

            if (describeDBClustersResult.getDBClusters().isEmpty()) {
                throw new IllegalStateException(String.format("Unable to find cluster %s", clusterId));
            }

            DBCluster dbCluster = describeDBClustersResult.getDBClusters().get(0);

            String clusterEndpoint = dbCluster.getEndpoint();
            String readerEndpoint = dbCluster.getReaderEndpoint();

            List<DBClusterMember> dbClusterMembers = dbCluster.getDBClusterMembers();
            Optional<DBClusterMember> clusterWriter = dbClusterMembers.stream()
                    .filter(DBClusterMember::isClusterWriter)
                    .findFirst();

            String primary = clusterWriter.map(DBClusterMember::getDBInstanceIdentifier).orElse("");
            List<String> replicas = dbClusterMembers.stream()
                    .filter(dbClusterMember -> !dbClusterMember.isClusterWriter())
                    .map(DBClusterMember::getDBInstanceIdentifier)
                    .collect(Collectors.toList());

            DescribeDBInstancesRequest describeDBInstancesRequest = new DescribeDBInstancesRequest()
                    .withFilters(Collections.singletonList(
                            new Filter()
                                    .withName("db-cluster-id")
                                    .withValues(dbCluster.getDBClusterIdentifier())));

            DescribeDBInstancesResult describeDBInstancesResult = neptune
                    .describeDBInstances(describeDBInstancesRequest);

            Collection<NeptuneInstanceMetadata> instances = new ArrayList<>();
            describeDBInstancesResult.getDBInstances()
                    .forEach(c -> {
                                String role = "unknown";
                                if (primary.equals(c.getDBInstanceIdentifier())) {
                                    role = "writer";
                                }
                                if (replicas.contains(c.getDBInstanceIdentifier())) {
                                    role = "reader";
                                }
                                String address = c.getEndpoint() == null ? null : c.getEndpoint().getAddress();
                                instances.add(
                                        new NeptuneInstanceMetadata()
                                                .withInstanceId(c.getDBInstanceIdentifier())
                                                .withRole(role)
                                                .withAddress(address)
                                                .withStatus(c.getDBInstanceStatus())
                                                .withAvailabilityZone(c.getAvailabilityZone())
                                                .withInstanceType(c.getDBInstanceClass())
                                                .withTags(getTags(c.getDBInstanceArn(), neptune)));
                            }
                    );

            neptune.shutdown();

            NeptuneClusterMetadata clusterMetadata = new NeptuneClusterMetadata()
                    .withInstances(instances)
                    .withClusterEndpoint(clusterEndpoint)
                    .withReaderEndpoint(readerEndpoint);

            cachedClusterMetadata.set(clusterMetadata);

            return clusterMetadata;

        } catch (AmazonNeptuneException e) {
            if (e.getErrorCode().equals("Throttling")) {
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

    private Map<String, String> getTags(String dbInstanceArn, AmazonNeptune neptune) {
        if (instanceTags.containsKey(dbInstanceArn)) {
            return instanceTags.get(dbInstanceArn);
        }

        List<Tag> tagList = neptune.listTagsForResource(
                new ListTagsForResourceRequest()
                        .withResourceName(dbInstanceArn)).getTagList();

        Map<String, String> tags = new HashMap<>();
        tagList.forEach(t -> tags.put(t.getKey(), t.getValue()));

        instanceTags.put(dbInstanceArn, tags);

        return tags;
    }
}
