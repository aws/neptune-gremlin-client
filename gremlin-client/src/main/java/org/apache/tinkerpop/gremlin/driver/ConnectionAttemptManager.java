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

package org.apache.tinkerpop.gremlin.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.utils.Clock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class ConnectionAttemptManager implements AutoCloseable {

    private final Refreshable client;
    private final AtomicBoolean refreshing;
    private final AtomicLong latestRefreshTime;
    private final int maxWaitForConnection;
    private final int eagerRefreshWaitTimeMillis;
    private final OnEagerRefresh onEagerRefresh;
    private final ExecutorService executorService;
    private final int eagerRefreshBackoffMillis;
    private final Clock clock;

    private static final Logger logger = LoggerFactory.getLogger(ConnectionAttemptManager.class);

    ConnectionAttemptManager(Refreshable client,
                             int maxWaitForConnection,
                             int eagerRefreshWaitTimeMillis,
                             OnEagerRefresh onEagerRefresh,
                             int eagerRefreshBackoffMillis,
                             Clock clock) {
        this(client,
                maxWaitForConnection,
                eagerRefreshWaitTimeMillis,
                onEagerRefresh,
                eagerRefreshBackoffMillis,
                clock,
                Executors.newSingleThreadExecutor(),
                0,
                false
        );
    }

    ConnectionAttemptManager(Refreshable client,
                             int maxWaitForConnection,
                             int eagerRefreshWaitTimeMillis,
                             OnEagerRefresh onEagerRefresh,
                             int eagerRefreshBackoffMillis,
                             Clock clock,
                             ExecutorService executorService,
                             long latestRefreshTime,
                             boolean isRefreshing) {
        this.client = client;
        this.maxWaitForConnection = maxWaitForConnection;
        this.eagerRefreshWaitTimeMillis = eagerRefreshWaitTimeMillis;
        this.onEagerRefresh = onEagerRefresh;
        this.eagerRefreshBackoffMillis = eagerRefreshBackoffMillis;
        this.clock = clock;
        this.executorService = executorService;
        this.latestRefreshTime = new AtomicLong(latestRefreshTime);
        this.refreshing = new AtomicBoolean(isRefreshing);

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
            return;
        }

        boolean isRefreshing = refreshing.get();

        if (!isRefreshing) {
            logger.warn("{} so getting new endpoints", message);
            executorService.submit(
                    new RefreshEventTask(context, client, refreshing, latestRefreshTime, onEagerRefresh, clock));
        } else {
            logger.warn("{} but already refreshing, so not getting new endpoints", message);
        }
    }

    private long waitTime(long start) {
        return clock.currentTimeMillis() - start;
    }

    public void shutdownNow() {
        executorService.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        shutdownNow();
    }

    static class RefreshEventTask implements Runnable {
        private final EagerRefreshContext context;
        private final Refreshable client;
        private final AtomicBoolean refreshing;
        private final AtomicLong latestRefreshTime;
        private final OnEagerRefresh onEagerRefresh;
        private final Clock clock;

        RefreshEventTask(EagerRefreshContext context,
                         Refreshable client,
                         AtomicBoolean refreshing,
                         AtomicLong latestRefreshTime,
                         OnEagerRefresh onEagerRefresh,
                         Clock clock) {
            this.context = context;
            this.client = client;
            this.refreshing = refreshing;
            this.latestRefreshTime = latestRefreshTime;
            this.onEagerRefresh = onEagerRefresh;
            this.clock = clock;
        }

        @Override
        public void run() {
            boolean allowRefresh = refreshing.compareAndSet(false, true);

            if (allowRefresh) {
                client.refreshEndpoints(onEagerRefresh.getEndpoints(context));
                refreshing.set(false);
                latestRefreshTime.getAndUpdate(currentValue -> Math.max(clock.currentTimeMillis(), currentValue));
            } else {
                logger.warn("Already refreshing, so taking no action");
            }
        }
    }
}
