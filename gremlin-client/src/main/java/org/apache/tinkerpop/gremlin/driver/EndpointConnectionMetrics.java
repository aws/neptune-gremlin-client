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

class EndpointConnectionMetrics {
    private final String address;
    private long succeeded = 0;
    private long unavailable = 0;
    private long closing = 0;
    private long dead = 0;
    private long npe = 0;
    private long nha = 0;
    private long minMillis = 0;
    private long maxMillis = 0;
    private long totalMillis;

    EndpointConnectionMetrics(String address) {
        this.address = address;
    }

    void succeeded(long startMillis){
        succeeded++;
        updateTimings(startMillis);
    }

    void unavailable(long startMillis){
        unavailable++;
        updateTimings(startMillis);
    }

    void closing(long startMillis){
        closing++;
        updateTimings(startMillis);
    }

    void dead(long startMillis){
        dead++;
        updateTimings(startMillis);
    }

    void npe(long startMillis){
        npe++;
        updateTimings(startMillis);
    }

    void nha(long startMillis){
        nha++;
        updateTimings(startMillis);
    }
    public String getAddress() {
        return address;
    }

    public long getSucceededCount() {
        return succeeded;
    }

    public long getUnavailableCount() {
        return unavailable;
    }

    public long getClosingCount() {
        return closing;
    }

    public long getDeadCount() {
        return dead;
    }

    public long getNullPointerExceptionCount() {
        return npe;
    }

    public long getNoHostsAvailableCount() {
        return nha;
    }

    public long getMinTimeToAcquireMillis() {
        return minMillis;
    }

    public long getMaxTimeToAcquireMillis() {
        return maxMillis;
    }

    public long getTotalAttempts(){
        return succeeded + unavailable + closing + dead + npe + nha;
    }

    public double getAverageTimeToAcquireMillis(){
        return (double)totalMillis/(double) getTotalAttempts();
    }

    private void updateTimings(long startMillis){
        long endMillis = System.currentTimeMillis();
        long duration = endMillis - startMillis;
        totalMillis += duration;
        if (duration > maxMillis){
            maxMillis = duration;
        }
        if (duration < minMillis){
            minMillis = duration;
        }
    }

    @Override
    public String toString() {
        return String.format("%s [total: %s, succeeded: %s, unavailable: %s, closing: %s, dead: %s, npe: %s, nha: %s, minMillis: %s, maxMillis: %s, avgMillis: %.2f]",
                getAddress(),
                getTotalAttempts(),
                getSucceededCount(),
                getUnavailableCount(),
                getClosingCount(),
                getDeadCount(),
                getNullPointerExceptionCount(),
                getNoHostsAvailableCount(),
                getMinTimeToAcquireMillis(),
                getMaxTimeToAcquireMillis(),
                getAverageTimeToAcquireMillis());
    }
}
