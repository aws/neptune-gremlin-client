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
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.*;
import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.AmazonNeptuneClientBuilder;
import com.amazonaws.services.neptune.model.ListTagsForResourceRequest;
import com.amazonaws.services.neptune.model.Tag;
import com.amazonaws.services.neptune.model.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.EndpointCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.RegionUtils;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

class GetEndpointsFromNeptuneManagementApi implements ClusterEndpointsFetchStrategy, ClusterMetadataSupplier {

    private static final Logger logger = LoggerFactory.getLogger(GetEndpointsFromNeptuneManagementApi.class);

    private final ClusterEndpointsFetchStrategy innerStrategy;
    private final String clusterId;
    private final String region;
    private final String iamProfile;
    private final AWSCredentialsProvider credentials;
    private final AtomicReference<NeptuneClusterMetadata> cachedClusterMetadata = new AtomicReference<>();
    private final ClientConfiguration clientConfiguration;
    private final boolean collectCloudWatchMetrics;

    GetEndpointsFromNeptuneManagementApi(String clusterId) {
        this(clusterId, RegionUtils.getCurrentRegionName());
    }


    GetEndpointsFromNeptuneManagementApi(String clusterId, String region) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE);
    }


    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, String iamProfile) {
        this(clusterId, region, iamProfile, null, null, false);
    }

    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, String iamProfile, ClientConfiguration clientConfiguration) {
        this(clusterId, region, iamProfile, null, clientConfiguration, false);
    }


    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, AWSCredentialsProvider credentials) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE, credentials, null, false);
    }

    GetEndpointsFromNeptuneManagementApi(String clusterId, String region, AWSCredentialsProvider credentials, ClientConfiguration clientConfiguration) {
        this(clusterId, region, IamAuthConfig.DEFAULT_PROFILE, credentials, clientConfiguration, false);
    }


    private GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                 String region,
                                                 String iamProfile,
                                                 AWSCredentialsProvider credentials,
                                                 ClientConfiguration clientConfiguration,
                                                 boolean collectCloudWatchMetrics) {
        this.innerStrategy = new CommonClusterEndpointsFetchStrategy(this);
        this.clusterId = clusterId;
        this.region = region;
        this.iamProfile = iamProfile;
        this.credentials = credentials;
        this.clientConfiguration = clientConfiguration;
        this.collectCloudWatchMetrics = collectCloudWatchMetrics;
    }

    @Override
    public NeptuneClusterMetadata refreshClusterMetadata() {
        try {
            AmazonNeptuneClientBuilder builder = AmazonNeptuneClientBuilder.standard();

            if (clientConfiguration != null) {
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

            List<NeptuneInstanceMetadata> instances = new ArrayList<>();
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

            if (collectCloudWatchMetrics) {
                addCloudWatchMetricsToInstances(instances);
            }

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

    private void addCloudWatchMetricsToInstances(List<NeptuneInstanceMetadata> instances) {
        AmazonCloudWatchClientBuilder builder = AmazonCloudWatchClientBuilder.standard();

        if (clientConfiguration != null) {
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

        AmazonCloudWatch cloudWatch = builder.build();

        GetMetricDataRequest getMetricDataRequest = new GetMetricDataRequest()
                .withStartTime(Date.from(Instant.now().minus(5, ChronoUnit.MINUTES)))
                .withEndTime(Date.from(Instant.now()))
                .withScanBy(ScanBy.TimestampDescending)
                .withMaxDatapoints(60);

        for (int i = 0; i < instances.size(); i++) {
            NeptuneInstanceMetadata instance = instances.get(i);
            getMetricDataRequest.withMetricDataQueries(
                    new MetricDataQuery()
                            .withId(Metric.CPUUtilization.name().toLowerCase() + "_" + i)
                            .withReturnData(true)
                            .withMetricStat(
                                    new MetricStat()
                                            .withStat("Average")
                                            .withPeriod(60)
                                            .withUnit(StandardUnit.Percent)
                                            .withMetric(
                                                    new com.amazonaws.services.cloudwatch.model.Metric()
                                                            .withMetricName(Metric.CPUUtilization.name())
                                                            .withNamespace("AWS/Neptune")
                                                            .withDimensions(
                                                                    new Dimension()
                                                                            .withName("DBInstanceIdentifier")
                                                                            .withValue(instance.getInstanceId())))),
                    new MetricDataQuery()
                            .withId(Metric.MainRequestQueuePendingRequests.name().toLowerCase() + "_" + i)
                            .withReturnData(true)
                            .withMetricStat(
                                    new MetricStat()
                                            .withStat("Average")
                                            .withPeriod(60)
                                            .withUnit(StandardUnit.Count)
                                            .withMetric(
                                                    new com.amazonaws.services.cloudwatch.model.Metric()
                                                            .withMetricName(Metric.MainRequestQueuePendingRequests.name())
                                                            .withNamespace("AWS/Neptune")
                                                            .withDimensions(
                                                                    new Dimension()
                                                                            .withName("DBInstanceIdentifier")
                                                                            .withValue(instance.getInstanceId()))))
            );

        }


        GetMetricDataResult metricData = cloudWatch.getMetricData(getMetricDataRequest);

        logger.debug("metricData: {}", metricData);

        List<MetricDataResult> metricDataResults = metricData.getMetricDataResults();

        if (metricDataResults.isEmpty()){
            logger.info("Unable to get CloudWatch metrics");
        } else {
            for (MetricDataResult metricDataResult : metricDataResults) {
                String[] parts = metricDataResult.getId().split("_");
                String metric = Metric.fromLower(parts[0]).name();
                int index = Integer.parseInt(parts[1]);
                NeptuneInstanceMetadata instance = instances.get(index);
                List<Double> values = metricDataResult.getValues();
                if (values.isEmpty()){
                    logger.warn("Missing {} metric for {}", metric, instance.getInstanceId());
                }else {
                    instance.setMetric(metric, values.get(0));
                }
            }
        }

        cloudWatch.shutdown();

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

        List<Tag> tagList = neptune.listTagsForResource(
                new ListTagsForResourceRequest()
                        .withResourceName(dbInstanceArn)).getTagList();

        Map<String, String> tags = new HashMap<>();
        tagList.forEach(t -> tags.put(t.getKey(), t.getValue()));

        return tags;
    }

    public static class Builder {
        private String clusterId;
        private String region;
        private String iamProfile = IamAuthConfig.DEFAULT_PROFILE;
        private AWSCredentialsProvider credentials;
        private ClientConfiguration clientConfiguration;
        private boolean collectCloudWatchMetrics;

        public Builder withClusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder withRegion(String region) {
            this.region = region;
            return this;
        }

        public Builder withIamProfile(String iamProfile) {
            this.iamProfile = iamProfile;
            return this;
        }

        public Builder withCredentials(AWSCredentialsProvider credentials) {
            this.credentials = credentials;
            return this;
        }

        public Builder withClientConfiguration(ClientConfiguration clientConfiguration) {
            this.clientConfiguration = clientConfiguration;
            return this;
        }

        public Builder withCollectCloudWatchMetrics(boolean collectCloudWatchMetrics) {
            this.collectCloudWatchMetrics = collectCloudWatchMetrics;
            return this;
        }

        public ClusterEndpointsRefreshAgent build() {
            GetEndpointsFromNeptuneManagementApi strategy =
                    new GetEndpointsFromNeptuneManagementApi(
                            clusterId,
                            region,
                            iamProfile,
                            credentials,
                            clientConfiguration,
                            collectCloudWatchMetrics);
            return new ClusterEndpointsRefreshAgent(strategy);
        }
    }
}
