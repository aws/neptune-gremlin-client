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

package software.amazon.neptune.cluster;

import org.apache.tinkerpop.gremlin.driver.GremlinClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class UnsuccessfulConnectAttemptManager {

    private final GremlinClient gremlinClient;
    private final AtomicBoolean refreshing = new AtomicBoolean(false);
    private final AtomicInteger attemptCount = new AtomicInteger(0);
    private final AtomicLong latestStartTime = new AtomicLong(0);
    private final int attemptCountThreshold;
    private final int waitTimeMillis;
    private final Supplier<EndpointCollection> endpointSupplier;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private static final Logger logger = LoggerFactory.getLogger(UnsuccessfulConnectAttemptManager.class);

    UnsuccessfulConnectAttemptManager(GremlinClient gremlinClient,
                                      int maxAttemptsToAcquireConnection,
                                      int maxTimeToAcquireConnectionMillis,
                                      Supplier<EndpointCollection> onFailureToAcquireConnection) {
        this.gremlinClient = gremlinClient;
        this.attemptCountThreshold = maxAttemptsToAcquireConnection;
        this.waitTimeMillis = maxTimeToAcquireConnectionMillis;
        this.endpointSupplier = onFailureToAcquireConnection;

        logger.info("maxAttemptsToAcquireConnection: {}, maxTimeToAcquireConnectionMillis: {}", attemptCountThreshold, waitTimeMillis);
    }

    public void handleUnsuccessfulConnectAttempt(long initialStartTime) {

        EvaluationResult evaluationResult = evaluateConditions(
                latestStartTime(initialStartTime),
                attemptCount.incrementAndGet());

        if (evaluationResult.thresholdExceeded()) {

            reset();

            if (endpointSupplier != null) {
                boolean isRefreshing = refreshing.get();
                if (!isRefreshing) {
                    logger.warn("{} so getting new endpoints", evaluationResult.message());
                    executorService.submit(new RefreshEventTask(gremlinClient, refreshing, endpointSupplier));
                } else {
                    logger.warn("{} but already refreshing, so not getting new endpoints", evaluationResult.message());
                }
            }
        }
    }

    public void reset() {
        attemptCount.set(0);
        latestStartTime.set(System.currentTimeMillis());
    }

    public void shutdownNow() {
        executorService.shutdownNow();
    }

    private EvaluationResult evaluateConditions(long startTime, int attemptCount) {
        if (waitTimeExceedsThreshold(startTime)) {
            return new EvaluationResult(true, String.format("Wait time to get connection has exceeded threshold [%s millis]", waitTimeMillis));
        }
        if (attemptCountExceedsThreshold(attemptCount)) {
            return new EvaluationResult(true, String.format("Number of unsuccessful connect attempts has exceeded threshold [%s]", attemptCountThreshold));
        }
        return new EvaluationResult(false, null);
    }

    private boolean attemptCountExceedsThreshold(int currentAttemptCount) {
        return attemptCountThreshold > 0 && currentAttemptCount > attemptCountThreshold;
    }

    private boolean waitTimeExceedsThreshold(long start) {
        return waitTimeMillis > 0 && (System.currentTimeMillis() - start) > waitTimeMillis;
    }

    private long latestStartTime(long start) {
        return Math.max(start, latestStartTime.get());
    }

    private static class EvaluationResult {
        private final boolean thresholdExceeded;
        private final String message;

        private EvaluationResult(boolean thresholdExceeded, String message) {
            this.thresholdExceeded = thresholdExceeded;
            this.message = message;
        }

        public boolean thresholdExceeded() {
            return thresholdExceeded;
        }

        public String message() {
            return message;
        }
    }

    private static class RefreshEventTask implements Runnable {

        private final GremlinClient client;
        private final AtomicBoolean refreshing;
        private final Supplier<EndpointCollection> endpointSupplier;

        private RefreshEventTask(GremlinClient client,
                                 AtomicBoolean refreshing,
                                 Supplier<EndpointCollection> endpointSupplier) {
            this.client = client;
            this.refreshing = refreshing;
            this.endpointSupplier = endpointSupplier;
        }

        @Override
        public void run() {
            boolean allowRefresh = refreshing.compareAndSet(false, true);

            if (allowRefresh) {
                client.refreshEndpoints(endpointSupplier.get());
                refreshing.set(false);
            } else {
                logger.warn("Already refreshing, so taking no action");
            }
        }
    }
}
