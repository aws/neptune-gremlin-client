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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public enum EndpointsType implements EndpointsSelector {

    ClusterEndpoint {
        @Override
        public EndpointCollection getEndpoints(NeptuneClusterMetadata clusterMetadata) {
            return new EndpointCollection(
                    Collections.singletonList(
                            clusterMetadata.getClusterEndpoint()));
        }
    },
    ReaderEndpoint {
        @Override
        public EndpointCollection getEndpoints(NeptuneClusterMetadata clusterMetadata) {
            return new EndpointCollection(
                    Collections.singletonList(
                            clusterMetadata.getReaderEndpoint()));
        }
    },
    All {
        @Override
        public EndpointCollection getEndpoints(NeptuneClusterMetadata clusterMetadata) {

            List<Endpoint> results = clusterMetadata.getInstances().stream()
                    .filter(NeptuneInstanceMetadata::isAvailable)
                    .collect(Collectors.toList());

            if (results.isEmpty()) {
                logger.warn("Unable to get any endpoints so getting ReaderEndpoint instead");
                return ReaderEndpoint.getEndpoints(clusterMetadata);
            }

            return new EndpointCollection(results);
        }
    },
    Primary {
        @Override
        public EndpointCollection getEndpoints(NeptuneClusterMetadata clusterMetadata) {

            List<Endpoint> results = clusterMetadata.getInstances().stream()
                    .filter(NeptuneInstanceMetadata::isPrimary)
                    .filter(NeptuneInstanceMetadata::isAvailable)
                    .collect(Collectors.toList());

            if (results.isEmpty()) {
                logger.warn("Unable to get Primary endpoint so getting ClusterEndpoint instead");
                return ClusterEndpoint.getEndpoints(clusterMetadata);
            }

            return new EndpointCollection(results);
        }
    },
    ReadReplicas {
        @Override
        public EndpointCollection getEndpoints(NeptuneClusterMetadata clusterMetadata) {

            List<Endpoint> results = clusterMetadata.getInstances().stream()
                    .filter(NeptuneInstanceMetadata::isReader)
                    .filter(NeptuneInstanceMetadata::isAvailable)
                    .collect(Collectors.toList());

            if (results.isEmpty()) {
                logger.warn("Unable to get ReadReplicas endpoints so getting ReaderEndpoint instead");
                return ReaderEndpoint.getEndpoints(clusterMetadata);
            }

            return new EndpointCollection(results);
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(EndpointsType.class);

}
