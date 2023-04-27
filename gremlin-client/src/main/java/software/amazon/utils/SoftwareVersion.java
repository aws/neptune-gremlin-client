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

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SoftwareVersion {
    public static final SoftwareVersion FromResource = SoftwareVersion.fromResource();

    private static final SoftwareVersion fromResource() {
        String name = SoftwareVersion.class.getPackage().getImplementationTitle();
        String version = SoftwareVersion.class.getPackage().getImplementationVersion();
        try {
            if (StringUtils.isEmpty(name) || StringUtils.isEmpty(version)) {
                // this is where we think the pom properties are on AWS Lambda
                File file = new File("/var/task/META-INF/maven/software.amazon.neptune/gremlin-client/pom.properties");
                if (file.exists()) {
                    try (InputStream filestream = new FileInputStream(file)) {
                        Properties properties = new Properties();
                        properties.load(filestream);
                        name = properties.getProperty("artifactId", "unknown");
                        version = properties.getProperty("version", "unknown");
                    } ;
                } else {
                    name = "unknown";
                    version = "unknown";
                }
            }
        } catch (IOException e) {
            // Do nothing
        }
        return new SoftwareVersion(name, version);
    }

    private final String name;
    private final String version;

    private SoftwareVersion(String name, String version) {
        this.name = name;
        this.version = version;
    }

    @Override
    public String toString() {
        return name + ":" + version;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version;
    }
}
