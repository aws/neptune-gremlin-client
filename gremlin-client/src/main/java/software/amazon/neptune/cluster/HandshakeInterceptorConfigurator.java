/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package software.amazon.neptune.cluster;

import org.apache.tinkerpop.gremlin.driver.*;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.stream.Collectors;

class HandshakeInterceptorConfigurator implements TopologyAwareBuilderConfigurator {

    private final boolean isDirectConnection;
    private final HandshakeInterceptor interceptor;
    private final boolean enableIamAuth;
    private final int port;
    private final int proxyPort;
    private final String proxyAddress;
    private final String serviceRegion;
    private final String iamProfile;
    private final AwsCredentialsProvider credentials;

    private final boolean removeHostHeader;

    HandshakeInterceptorConfigurator(boolean isDirectConnection,
                                     HandshakeInterceptor interceptor,
                                     boolean enableIamAuth,
                                     int port,
                                     int proxyPort,
                                     String proxyAddress,
                                     String serviceRegion,
                                     String iamProfile,
                                     AwsCredentialsProvider credentials,
                                     boolean removeHostHeader) {
        this.isDirectConnection = isDirectConnection;
        this.interceptor = interceptor;
        this.enableIamAuth = enableIamAuth;
        this.port = port;
        this.proxyPort = proxyPort;
        this.proxyAddress = proxyAddress;
        this.serviceRegion = serviceRegion;
        this.iamProfile = iamProfile;
        this.credentials = credentials;
        this.removeHostHeader = removeHostHeader;
    }

    @Override
    public void apply(Cluster.Builder builder, EndpointCollection endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return;
        }

        if (isDirectConnection) {
            builder.port(port);
            for (Endpoint endpoint : endpoints) {
                builder.addContactPoint(endpoint.getAddress());
            }
        } else {
            builder.port(proxyPort);
            if (proxyAddress != null) {
                builder.addContactPoint(proxyAddress);
            }
        }

        if (interceptor != null) {
            builder.handshakeInterceptor(interceptor);
        } else {

            IamAuthConfig.IamAuthConfigBuilder iamAuthConfigBuilder =
                    IamAuthConfig.builder()
                            .addNeptuneEndpoints(endpoints.stream().map(Endpoint::getAddress).collect(Collectors.toList()))
                            .setNeptunePort(port)
                            .setServiceRegion(serviceRegion)
                            .setIamProfile(iamProfile)
                            .setCredentials(credentials);

            if (enableIamAuth) {
                iamAuthConfigBuilder.enableIamAuth();
            }

            if (!isDirectConnection) {
                iamAuthConfigBuilder.connectViaLoadBalancer();
            }

            if (removeHostHeader) {
                iamAuthConfigBuilder.removeHostHeaderAfterSigning();
            }

            IamAuthConfig iamAuthConfig = iamAuthConfigBuilder.build();

            builder.handshakeInterceptor(new LBAwareHandshakeInterceptor(iamAuthConfig));
        }
    }
}
