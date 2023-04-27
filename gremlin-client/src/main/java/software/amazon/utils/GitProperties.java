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

package software.amazon.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GitProperties {

    public static final GitProperties FromResource = GitProperties.fromResource();

    private final String commitId;
    private final String buildVersion;
    private final String commitTime;
    private final String buildTime;

    private GitProperties(String commitId, String buildVersion, String commitTime, String buildTime) {
        this.commitId = commitId;
        this.buildVersion = buildVersion;
        this.commitTime = commitTime;
        this.buildTime = buildTime;
    }

    private static GitProperties fromResource() {
        Properties properties = new Properties();
        try {

            InputStream stream = ClassLoader.getSystemResourceAsStream("git.properties");
            if (stream != null) {
                properties.load(stream);
                stream.close();
            } else {
                // this is where we think the git properties are on AWS Lambda
                File file = new File("/var/task/git.properties");
                if (file.exists()){
                    try (InputStream filestream =  new FileInputStream(file)){
                        properties.load(filestream);
                    };
                }
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
                "]";
    }

    public String getCommitId() {
        return commitId;
    }

    public String getBuildVersion() {
        return buildVersion;
    }

    public String getCommitTime() {
        return commitTime;
    }

    public String getBuildTime() {
        return buildTime;
    }
}
