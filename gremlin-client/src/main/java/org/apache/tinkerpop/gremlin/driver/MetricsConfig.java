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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.EnvironmentVariableUtils;

class MetricsConfig {

    private static final String PROPERTY_NAME = "org.apache.tinkerpop.gremlin.driver.MetricsConfig.enableMetrics";

    private static final Logger logger = LoggerFactory.getLogger(MetricsConfig.class);
    private final boolean enableMetrics;
    private final MetricsHandlerCollection metricsHandlers;

    MetricsConfig(boolean enableMetrics, MetricsHandlerCollection metricsHandlers) {
        this.enableMetrics = calculateEnableMetricsValue(enableMetrics);
        this.metricsHandlers = metricsHandlers;
    }

    public boolean enableMetrics() {
        return enableMetrics;
    }

    public MetricsHandlerCollection metricsHandlers() {
        return metricsHandlers;
    }

    private boolean calculateEnableMetricsValue(boolean enableMetricsBuilder) {

        Boolean enableMetricsEnv = null;
        Boolean enableMetricsSys = null;

        String envVar = EnvironmentVariableUtils.getOptionalEnv(PROPERTY_NAME, null);
        if (!(envVar == null || envVar.isEmpty())) {
            enableMetricsEnv = Boolean.parseBoolean(envVar);
        }

        String sysProp = System.getProperty(PROPERTY_NAME, null);
        if (!(sysProp == null || sysProp.isEmpty())) {
            enableMetricsSys = Boolean.parseBoolean(sysProp);
        }

        boolean result = false;

        if ((enableMetricsEnv != null && !enableMetricsEnv) || (enableMetricsSys != null && !enableMetricsSys)) {
            result = false;
        } else if (enableMetricsBuilder || enableMetricsEnv != null || enableMetricsSys != null) {
            result = true;
        }

        logger.debug("Enable metrics: {} [builder: {}, env: {}, sys: {}]", result, enableMetricsBuilder, enableMetricsEnv, enableMetricsSys);

        return result;
    }
}
