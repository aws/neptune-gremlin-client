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

public class EndpointRequestMetrics {
    private final long start = System.currentTimeMillis();

    private final String address;
    private long totalDurationMillis;
    private long minMillis = Long.MAX_VALUE;
    private long maxMillis = 0L;
    private long count;

    public EndpointRequestMetrics(String address) {
        this.address = address;
    }

    public void update(long duration) {
        totalDurationMillis += duration;
        if (duration > maxMillis) {
            maxMillis = duration;
        }
        if (duration < minMillis) {
            minMillis = duration;
        }
        count++;
    }

    public String getAddress() {
        return address;
    }

    public long getCount() {
        return count;
    }


    public double getRatePerSecond() {

        long duration = System.currentTimeMillis() - start;
        return (double) count / ((double) (duration) / 1000.00);

    }

    public double getAverageLatencyMillis() {
        return (double) totalDurationMillis / (double) count;
    }

    public long getMinLatencyMillis() {
        return minMillis;
    }

    public long getMaxLatencyMillis() {
        return maxMillis;
    }

    @Override
    public String toString() {
        return String.format("%s [count: %s, ratePerSec: %.3f, minMillis: %s, maxMillis: %s, avgMillis: %.2f]",
                getAddress(),
                getCount(),
                getRatePerSecond(),
                getMinLatencyMillis(),
                getMaxLatencyMillis(),
                getAverageLatencyMillis());
    }
}
