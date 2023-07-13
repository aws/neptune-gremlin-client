package org.apache.tinkerpop.gremlin.driver;

public class EndpointRequestMetrics {
    private final long start = System.currentTimeMillis();

    private final String address;
    private volatile long totalDurationMillis;
    private volatile long minMillis = Long.MAX_VALUE;
    private volatile long maxMillis = 0L;
    private volatile long count;

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

    public long count() {
        return count;
    }


    public double ratePerSecond() {

        long duration = System.currentTimeMillis() - start;
        return (double) count / ((double) (duration) / 1000.00);

    }

    public double averageLatencyMillis() {
        return (double) totalDurationMillis / (double) count;
    }

    @Override
    public String toString() {
        return String.format("%s [count: %s, ratePerSec: %.3f, minMillis: %s, maxMillis: %s, avgMillis: %.2f]",
                address,
                count,
                ratePerSecond(),
                minMillis,
                maxMillis,
                averageLatencyMillis());
    }
}
