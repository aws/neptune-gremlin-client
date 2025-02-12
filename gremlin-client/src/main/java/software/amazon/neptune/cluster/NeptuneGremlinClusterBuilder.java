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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.neptune.auth.credentials.V1toV2CredentialsProvider;
import io.netty.handler.ssl.SslContext;
import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.apache.tinkerpop.gremlin.util.*;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.*;
import java.util.function.Supplier;

public class NeptuneGremlinClusterBuilder {

    public static NeptuneGremlinClusterBuilder build() {
        return new NeptuneGremlinClusterBuilder();
    }

    private final GremlinClusterBuilder innerBuilder = GremlinClusterBuilder.build();

    private List<Endpoint> endpoints = new ArrayList<>();
    private String proxyAddress;

    private boolean removeHostHeader = false;
    private boolean enableSsl = true;
    private boolean enableIamAuth = false;
    private int port = 8182;
    private int proxyPort = 80;
    private String iamProfile = IamAuthConfig.DEFAULT_PROFILE;
    private String serviceRegion = "";
    private HandshakeInterceptor interceptor = null;
    private AwsCredentialsProvider credentials = null;
    private EndpointFilter endpointFilter = new SuspendedEndpoints();

    private NeptuneGremlinClusterBuilder() {
    }

    public NeptuneGremlinClusterBuilder addMetricsHandler(MetricsHandler handler){
        innerBuilder.addMetricsHandler(handler);
        return this;
    }

    public NeptuneGremlinClusterBuilder enableMetrics(boolean enableMetrics){
        innerBuilder.enableMetrics(enableMetrics);
        return this;
    }

    /**
     * Number of millis to wait between each attempt to acquire a connection.
     */
    public NeptuneGremlinClusterBuilder acquireConnectionBackoffMillis(final int acquireConnectionBackoffMillis) {
        innerBuilder.acquireConnectionBackoffMillis(acquireConnectionBackoffMillis);
        return this;
    }

    /**
     * Minimum number of millis to wait between invoking handler supplied in
     * {@link #onEagerRefresh}.
     */
    public NeptuneGremlinClusterBuilder eagerRefreshBackoffMillis(final int eagerRefreshBackoffMillis) {
        innerBuilder.eagerRefreshBackoffMillis(eagerRefreshBackoffMillis);
        return this;
    }

    /**
     * Number of millis to wait while trying to acquire connection before invoking handler supplied in
     * {@link #onEagerRefresh}.
     */
    public NeptuneGremlinClusterBuilder eagerRefreshWaitTimeMillis(final int eagerRefreshWaitTimeMillis) {
        innerBuilder.eagerRefreshWaitTimeMillis(eagerRefreshWaitTimeMillis);
        return this;
    }

    /**
     * Handler to be invoked after {@link #eagerRefreshWaitTimeMillis}.
     * The handler should return a {@link Supplier< EndpointCollection >}.
     */
    public NeptuneGremlinClusterBuilder onEagerRefresh(final OnEagerRefresh eventHandler) {
        innerBuilder.onEagerRefresh(eventHandler);
        return this;
    }

    /**
     * Strategy for filtering and enriching available endpoints before creating clients.
     */
    public NeptuneGremlinClusterBuilder endpointFilter(EndpointFilter endpointFilter) {
        this.endpointFilter = endpointFilter;
        return this;
    }

    public NeptuneGremlinClusterBuilder nioPoolSize(final int nioPoolSize) {
        innerBuilder.nioPoolSize(nioPoolSize);
        return this;
    }

    public NeptuneGremlinClusterBuilder workerPoolSize(final int workerPoolSize) {
        innerBuilder.workerPoolSize(workerPoolSize);
        return this;
    }

    public NeptuneGremlinClusterBuilder path(final String path) {
        innerBuilder.path(path);
        return this;
    }

    public NeptuneGremlinClusterBuilder serializer(final String mimeType) {
        innerBuilder.serializer(mimeType);
        return this;
    }

    public NeptuneGremlinClusterBuilder serializer(final Serializers mimeType) {
        innerBuilder.serializer(mimeType);
        return this;
    }

    public NeptuneGremlinClusterBuilder serializer(final MessageSerializer serializer) {
        innerBuilder.serializer(serializer);
        return this;
    }

    /**
     * Enables connectivity over SSL - default 'true' for Amazon Neptune clusters.
     */
    public NeptuneGremlinClusterBuilder enableSsl(final boolean enable) {
        this.enableSsl = enable;
        return this;
    }

    public NeptuneGremlinClusterBuilder sslContext(final SslContext sslContext) {
        innerBuilder.sslContext(sslContext);
        return this;
    }

    public NeptuneGremlinClusterBuilder keepAliveInterval(final long keepAliveInterval) {
        innerBuilder.keepAliveInterval(keepAliveInterval);
        return this;
    }

    public NeptuneGremlinClusterBuilder keyStore(final String keyStore) {
        innerBuilder.keyStore(keyStore);
        return this;
    }

    public NeptuneGremlinClusterBuilder keyStorePassword(final String keyStorePassword) {
        innerBuilder.keyStorePassword(keyStorePassword);
        return this;
    }

    public NeptuneGremlinClusterBuilder trustStore(final String trustStore) {
        innerBuilder.trustStore(trustStore);
        return this;
    }

    public NeptuneGremlinClusterBuilder trustStorePassword(final String trustStorePassword) {
        innerBuilder.trustStorePassword(trustStorePassword);
        return this;
    }

    public NeptuneGremlinClusterBuilder keyStoreType(final String keyStoreType) {
        innerBuilder.keyStoreType(keyStoreType);
        return this;
    }

    public NeptuneGremlinClusterBuilder sslEnabledProtocols(final List<String> sslEnabledProtocols) {
        innerBuilder.sslEnabledProtocols(sslEnabledProtocols);
        return this;
    }

    public NeptuneGremlinClusterBuilder sslCipherSuites(final List<String> sslCipherSuites) {
        innerBuilder.sslCipherSuites(sslCipherSuites);
        return this;
    }

    public NeptuneGremlinClusterBuilder sslSkipCertValidation(final boolean sslSkipCertValidation) {
        innerBuilder.sslSkipCertValidation(sslSkipCertValidation);
        return this;
    }

    public NeptuneGremlinClusterBuilder minInProcessPerConnection(final int minInProcessPerConnection) {
        innerBuilder.minInProcessPerConnection(minInProcessPerConnection);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxInProcessPerConnection(final int maxInProcessPerConnection) {
        innerBuilder.maxInProcessPerConnection(maxInProcessPerConnection);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxSimultaneousUsagePerConnection(final int maxSimultaneousUsagePerConnection) {
        innerBuilder.maxSimultaneousUsagePerConnection(maxSimultaneousUsagePerConnection);
        return this;
    }

    public NeptuneGremlinClusterBuilder minSimultaneousUsagePerConnection(final int minSimultaneousUsagePerConnection) {
        innerBuilder.minSimultaneousUsagePerConnection(minSimultaneousUsagePerConnection);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxConnectionPoolSize(final int maxSize) {
        innerBuilder.maxConnectionPoolSize(maxSize);
        return this;
    }

    public NeptuneGremlinClusterBuilder minConnectionPoolSize(final int minSize) {
        innerBuilder.minConnectionPoolSize(minSize);
        return this;
    }

    public NeptuneGremlinClusterBuilder resultIterationBatchSize(final int size) {
        innerBuilder.resultIterationBatchSize(size);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxWaitForConnection(final int maxWait) {
        innerBuilder.maxWaitForConnection(maxWait);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxWaitForClose(final int maxWait) {
        innerBuilder.maxWaitForClose(maxWait);
        return this;
    }

    public NeptuneGremlinClusterBuilder maxContentLength(final int maxContentLength) {
        innerBuilder.maxContentLength(maxContentLength);
        return this;
    }

    public NeptuneGremlinClusterBuilder channelizer(final String channelizerClass) {
        innerBuilder.channelizer(channelizerClass);
        return this;
    }

    public NeptuneGremlinClusterBuilder channelizer(final Class channelizerClass) {
        return channelizer(channelizerClass.getCanonicalName());
    }

    public NeptuneGremlinClusterBuilder validationRequest(final String script) {
        innerBuilder.validationRequest(script);
        return this;
    }

    public NeptuneGremlinClusterBuilder reconnectInterval(final int interval) {
        innerBuilder.reconnectInterval(interval);
        return this;
    }

    public NeptuneGremlinClusterBuilder loadBalancingStrategy(final Supplier<LoadBalancingStrategy> loadBalancingStrategy) {
        innerBuilder.loadBalancingStrategy(loadBalancingStrategy);
        return this;
    }

    public NeptuneGremlinClusterBuilder authProperties(final AuthProperties authProps) {
        innerBuilder.authProperties(authProps);
        return this;
    }

    public NeptuneGremlinClusterBuilder credentials(final String username, final String password) {
        innerBuilder.credentials(username, password);
        return this;
    }

    public NeptuneGremlinClusterBuilder protocol(final String protocol) {
        innerBuilder.protocol(protocol);
        return this;
    }

    public NeptuneGremlinClusterBuilder jaasEntry(final String jaasEntry) {
        innerBuilder.jaasEntry(jaasEntry);
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPoint(final String address) {
        this.endpoints.add(new DatabaseEndpoint().withAddress(address));
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPoints(final String... addresses) {
        for (String address : addresses)
            addContactPoint(address);
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPoints(final Collection<String> addresses) {
        for (String address : addresses)
            addContactPoint(address);
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPoint(final Endpoint endpoint) {
        this.endpoints.add(endpoint);
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPoints(final EndpointCollection endpointCollection) {
        for (Endpoint endpoint : endpointCollection)
            addContactPoint(endpoint);
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPointsMetadata(final Endpoint... endpointCollection) {
        for (Endpoint endpoint : endpointCollection)
            addContactPoint(endpoint);
        return this;
    }

    public NeptuneGremlinClusterBuilder addContactPointsMetadata(final Collection<Endpoint> endpointCollection) {
        for (Endpoint endpoint : endpointCollection)
            addContactPoint(endpoint);
        return this;
    }

    public NeptuneGremlinClusterBuilder port(final int port) {
        this.port = port;
        return this;
    }

    public NeptuneGremlinClusterBuilder proxyPort(final int port) {
        this.proxyPort = port;
        return this;
    }

    public NeptuneGremlinClusterBuilder proxyAddress(final String address) {
        this.proxyAddress = address;
        return this;
    }

    public NeptuneGremlinClusterBuilder proxyRemoveHostHeader(final boolean removeHostHeader) {
        this.removeHostHeader = removeHostHeader;
        return this;
    }

    public NeptuneGremlinClusterBuilder enableIamAuth(final boolean enable) {
        this.enableIamAuth = enable;
        return this;
    }

    public NeptuneGremlinClusterBuilder serviceRegion(final String serviceRegion) {
        this.serviceRegion = serviceRegion;
        return this;
    }

    public NeptuneGremlinClusterBuilder iamProfile(final String iamProfile) {
        this.iamProfile = iamProfile;
        return this;
    }


    public NeptuneGremlinClusterBuilder handshakeInterceptor(final HandshakeInterceptor interceptor) {
        this.interceptor = interceptor;
        return this;
    }

    public NeptuneGremlinClusterBuilder credentials(final AWSCredentialsProvider v1AwsCredentialProvider) {
        this.credentials = V1toV2CredentialsProvider.create(v1AwsCredentialProvider);
        return this;
    }

    public NeptuneGremlinClusterBuilder credentials(final AwsCredentialsProvider credentials) {
        this.credentials = credentials;
        return this;
    }

    private boolean isDirectConnection() {
        return proxyAddress == null;
    }


    public GremlinCluster create() {

        innerBuilder.enableSsl(this.enableSsl);

        Collection<Endpoint> filteredEndpoints = new ArrayList<>();
        Set<String> rejectedReasons = new HashSet<>();

        if (endpointFilter != null) {
            innerBuilder.endpointFilter(endpointFilter);
            for (Endpoint endpoint : endpoints) {
                ApprovalResult approvalResult = endpointFilter.approveEndpoint(endpoint);
                if (approvalResult.isApproved()) {
                    filteredEndpoints.add(endpoint);
                } else {
                    rejectedReasons.add(approvalResult.reason());
                }
            }
        } else {
            filteredEndpoints.addAll(endpoints);
        }

        if (filteredEndpoints.isEmpty()) {
            if (!rejectedReasons.isEmpty()) {
                throw new EndpointsUnavailableException(rejectedReasons);
            }
            if (isDirectConnection()) {
                throw new IllegalStateException("The list of endpoint addresses is empty. You must supply one or more endpoints.");
            } else if (enableIamAuth) {
                throw new IllegalStateException("The list of endpoint addresses is empty. You must supply one or more endpoints to sign the Host header.");
            }
        }

        for (Endpoint endpoint : filteredEndpoints) {
            innerBuilder.addContactPoint(endpoint);
        }

        TopologyAwareBuilderConfigurator configurator = new HandshakeInterceptorConfigurator(
                isDirectConnection(),
                interceptor,
                enableIamAuth,
                port,
                proxyPort,
                proxyAddress,
                serviceRegion,
                iamProfile,
                credentials,
                removeHostHeader
        );

        innerBuilder.topologyAwareBuilderConfigurator(configurator);

        return innerBuilder.create();
    }
}
