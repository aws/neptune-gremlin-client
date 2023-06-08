package software.amazon.neptune.cluster;

enum Metric {
    MainRequestQueuePendingRequests,
    CPUUtilization;

    public static Metric fromLower(String value){
        for (Metric metric : values()) {
            if (metric.name().equalsIgnoreCase(value)){
                return metric;
            }
        }
        throw new IllegalArgumentException(String.format("Unrecognized metric name: %s", value));
    }
}
