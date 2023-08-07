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

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class EndpointRequestMetrics {
    private final long start = System.currentTimeMillis();

    private final String address;
    private long totalDurationMillis;
    private long minMillis = 0L;
    private long maxMillis = 0L;
    private long successCount;
    private long errorCount;
    private final ConcurrentHashMap<Class<? extends Throwable>, ErrorMetric> errors = new ConcurrentHashMap<>();

    public EndpointRequestMetrics(String address) {
        this.address = address;
    }

    public void update(long duration, Throwable e) {
        totalDurationMillis += duration;
        if (duration > maxMillis) {
            maxMillis = duration;
        }
        if (duration < minMillis) {
            minMillis = duration;
        }
        if (e == null) {
            successCount++;
        } else {
            errors.compute(e.getClass(), new BiFunction<Class<? extends Throwable>, ErrorMetric, ErrorMetric>() {
                @Override
                public ErrorMetric apply(Class<? extends Throwable> aClass, ErrorMetric errorMetric) {
                    if (errorMetric == null){
                        return new ErrorMetric(aClass).increment();
                    } else {
                        return errorMetric.increment();
                    }
                }
            });
            errorCount++;
        }
    }

    public String getAddress() {
        return address;
    }

    public long getSuccessCount() {
        return successCount;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public double getRatePerSecond() {

        long duration = System.currentTimeMillis() - start;
        return (double) successCount / ((double) (duration) / 1000.00);

    }

    public double getAverageLatencyMillis() {
        return (double) totalDurationMillis / (double) successCount;
    }

    public long getMinLatencyMillis() {
        return minMillis;
    }

    public long getMaxLatencyMillis() {
        return maxMillis;
    }

    public Collection<ErrorMetric> getErrors(){
        return errors.values();
    }

    @Override
    public String toString() {

        String errorString = getErrors().isEmpty() ?
                "" :
                String.format(", errors: [%s]", getErrors().stream().map(ErrorMetric::toString).collect(Collectors.joining(", ")));

        return String.format("%s [succeeded: %s, failed: %s, ratePerSec: %.3f, minMillis: %s, maxMillis: %s, avgMillis: %.2f%s]",
                getAddress(),
                getSuccessCount(),
                getErrorCount(),
                getRatePerSecond(),
                getMinLatencyMillis(),
                getMaxLatencyMillis(),
                getAverageLatencyMillis(),
                errorString
        );
    }
}
