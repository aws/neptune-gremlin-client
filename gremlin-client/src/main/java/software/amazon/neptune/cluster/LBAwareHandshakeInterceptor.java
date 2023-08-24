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

import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import io.netty.handler.codec.http.FullHttpRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.driver.HandshakeInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.RegionUtils;

class LBAwareHandshakeInterceptor implements HandshakeInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(LBAwareHandshakeInterceptor.class);

    private final IamAuthConfig iamAuthConfig;
    private final String serviceRegion;

    private final NeptuneNettyHttpSigV4Signer sigV4Signer;

    LBAwareHandshakeInterceptor(IamAuthConfig iamAuthConfig) {
        this.iamAuthConfig = iamAuthConfig;
        this.serviceRegion = getServiceRegion();
        this.sigV4Signer = createSigV4Signer();
    }

    private NeptuneNettyHttpSigV4Signer createSigV4Signer() {
        if (iamAuthConfig.enableIamAuth()) {

            try {
                return new NeptuneNettyHttpSigV4Signer(
                        serviceRegion,
                        iamAuthConfig.credentialsProviderChain());


            } catch (NeptuneSigV4SignerException e) {
                throw new RuntimeException("Exception occurred while creating NeptuneSigV4Signer", e);
            }
        } else {
            return null;
        }
    }

    @Override
    public FullHttpRequest apply(FullHttpRequest request) {
        logger.trace("iamAuthConfig: {}, serviceRegion: {}", iamAuthConfig, serviceRegion);

        if (iamAuthConfig.enableIamAuth() || iamAuthConfig.connectViaLoadBalancer()) {
            request.headers().remove("Host");
            request.headers().remove("host");
            request.headers().add("Host", iamAuthConfig.chooseHostHeader());
        }

        if (iamAuthConfig.enableIamAuth()) {

            try {

                NeptuneNettyHttpSigV4Signer signer = sigV4Signer != null ?
                        sigV4Signer :
                        new NeptuneNettyHttpSigV4Signer(
                                serviceRegion,
                                iamAuthConfig.credentialsProviderChain());

                signer.signRequest(request);

                if (iamAuthConfig.removeHostHeaderAfterSigning()) {
                    request.headers().remove("Host");
                }


            } catch (NeptuneSigV4SignerException e) {
                throw new RuntimeException("Exception occurred while signing the request", e);
            }
        }

        return request;
    }

    private String getServiceRegion() {

        if (StringUtils.isNotEmpty(iamAuthConfig.serviceRegion())) {
            logger.debug("Using service region supplied in config");
            return iamAuthConfig.serviceRegion();
        } else if (StringUtils.isNotEmpty(System.getenv("SERVICE_REGION"))) {
            logger.debug("Using SERVICE_REGION environment variable as service region");
            return StringUtils.trim(System.getenv("SERVICE_REGION"));
        } else if (StringUtils.isNotEmpty(System.getProperty("SERVICE_REGION"))) {
            logger.debug("Using SERVICE_REGION system property as service region");
            return StringUtils.trim(System.getProperty("SERVICE_REGION"));
        } else {
            String currentRegionName = RegionUtils.getCurrentRegionName();
            if (currentRegionName != null) {
                logger.debug("Using current region as service region");
                return currentRegionName;
            } else {
                throw new IllegalStateException("Unable to determine Neptune service region. Use the SERVICE_REGION environment variable or system property, or the NeptuneGremlinClusterBuilder.serviceRegion() method to specify the Neptune service region.");
            }
        }

    }
}
