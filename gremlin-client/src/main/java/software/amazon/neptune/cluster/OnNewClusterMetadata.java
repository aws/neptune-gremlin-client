package software.amazon.neptune.cluster;

public interface OnNewClusterMetadata {
    void apply(NeptuneClusterMetadata neptuneClusterMetadata);
}
