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
import java.util.concurrent.atomic.AtomicLong;

public class ConnectionAttemptManager {

    private final GremlinClient gremlinClient;
    private final AtomicBoolean refreshing = new AtomicBoolean(false);
    private final AtomicLong latestRefreshTime = new AtomicLong(0);
    private final int maxWaitForConnection;
    private final int eagerRefreshWaitTimeMillis;
    private final OnEagerRefresh onEagerRefresh;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final int eagerRefreshBackoffMillis;

    private static final Logger logger = LoggerFactory.getLogger(ConnectionAttemptManager.class);

    ConnectionAttemptManager(GremlinClient gremlinClient,
                             int maxWaitForConnection,
                             int eagerRefreshWaitTimeMillis,
                             OnEagerRefresh onEagerRefresh,
                             int eagerRefreshBackoffMillis) {
        this.gremlinClient = gremlinClient;
        this.maxWaitForConnection = maxWaitForConnection;
        this.eagerRefreshWaitTimeMillis = eagerRefreshWaitTimeMillis;
        this.onEagerRefresh = onEagerRefresh;
        this.eagerRefreshBackoffMillis = eagerRefreshBackoffMillis;

        logger.info("maxWaitForConnection: {}, eagerRefreshWaitTimeMillis: {}, eagerRefreshBackoffMillis: {}",
                this.maxWaitForConnection,
                this.eagerRefreshWaitTimeMillis,
                this.eagerRefreshBackoffMillis);
    }

    public boolean maxWaitTimeExceeded(long start) {
        return waitTime(start) > maxWaitForConnection;
    }

    public boolean eagerRefreshWaitTimeExceeded(long start) {
        return eagerRefreshWaitTimeMillis > 0 && waitTime(start) > eagerRefreshWaitTimeMillis;
    }

    public void triggerEagerRefresh(EagerRefreshContext context) {

        String message = String.format("Wait time to get connection has exceeded threshold [%s millis]", eagerRefreshWaitTimeMillis);

        if (onEagerRefresh == null) {
            return;
        }

        long lastRefreshTime = latestRefreshTime.get();

        if (lastRefreshTime > 0 && waitTime(lastRefreshTime) < eagerRefreshBackoffMillis) {
            logger.warn("{} but last refresh occurred within backoff interval, so not getting new endpoints", message);
        }

        boolean isRefreshing = refreshing.get();

        if (!isRefreshing) {
            logger.warn("{} so getting new endpoints", message);
            executorService.submit(
                    new RefreshEventTask(gremlinClient, refreshing, latestRefreshTime, onEagerRefresh, context));
        } else {
            logger.warn("{} but already refreshing, so not getting new endpoints", message);
        }
    }

    private static long waitTime(long start) {
        return System.currentTimeMillis() - start;
    }

    public void shutdownNow() {
        executorService.shutdownNow();
    }

    private static class RefreshEventTask implements Runnable {

        private final GremlinClient client;
        private final AtomicBoolean refreshing;
        private final AtomicLong latestRefreshTime;
        private final OnEagerRefresh onEagerRefresh;
        private final EagerRefreshContext context;

        private RefreshEventTask(GremlinClient client,
                                 AtomicBoolean refreshing,
                                 AtomicLong latestRefreshTime,
                                 OnEagerRefresh onEagerRefresh,
                                 EagerRefreshContext context) {
            this.client = client;
            this.refreshing = refreshing;
            this.latestRefreshTime = latestRefreshTime;
            this.onEagerRefresh = onEagerRefresh;
            this.context = context;
        }

        @Override
        public void run() {
            boolean allowRefresh = refreshing.compareAndSet(false, true);

            if (allowRefresh) {
                client.refreshEndpoints(onEagerRefresh.getEndpoints(context));
                refreshing.set(false);
                latestRefreshTime.getAndUpdate(currentValue -> Math.max(System.currentTimeMillis(), currentValue));
            } else {
                logger.warn("Already refreshing, so taking no action");
            }
        }
    }
}
