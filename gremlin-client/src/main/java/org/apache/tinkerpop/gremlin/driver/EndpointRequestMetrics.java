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
