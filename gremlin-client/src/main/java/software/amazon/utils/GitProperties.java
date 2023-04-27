package software.amazon.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GitProperties {

    private final String commitId;
    private final String buildVersion;
    private final String commitTime;
    private final String buildTime;

    public GitProperties(String commitId, String buildVersion, String commitTime, String buildTime) {
        this.commitId = commitId;
        this.buildVersion = buildVersion;
        this.commitTime = commitTime;
        this.buildTime = buildTime;
    }

    public String commitId() {
        return commitId;
    }

    public static GitProperties fromResource() {
        Properties properties = new Properties();
        try {

            InputStream stream = ClassLoader.getSystemResourceAsStream("git.properties");
            if (stream != null) {
                properties.load(stream);
                stream.close();
            }
        } catch (IOException e) {
            // Do nothing
        }
        return new GitProperties(
                properties.getProperty("git.commit.id", "unknown"),
                properties.getProperty("git.build.version", "unknown"),
                properties.getProperty("git.commit.time", "unknown"),
                properties.getProperty("git.build.time", "unknown"));
    }

    @Override
    public String toString() {
        return "[" +
                "buildVersion='" + buildVersion + '\'' +
                ", buildTime='" + buildTime + '\'' +
                ", commitId='" + commitId + '\'' +
                ", commitTime='" + commitTime + '\'' +
                ']';
    }
}
