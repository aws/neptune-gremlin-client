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

package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;
import software.amazon.utils.Clock;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ConnectionAttemptManagerTest {

    @Test
    public void determinesIfMaxWaitTimeExceeded() throws Exception {

        Clock clock = mock(Clock.class);
        when(clock.currentTimeMillis()).thenReturn(5000L);

        try (ConnectionAttemptManager connectionAttemptManager = new ConnectionAttemptManager(
                null,
                1000,
                -1,
                null,
                -1,
                clock)) {


            assertFalse(connectionAttemptManager.maxWaitTimeExceeded(4500L));
            assertFalse(connectionAttemptManager.maxWaitTimeExceeded(4000L));

            assertTrue(connectionAttemptManager.maxWaitTimeExceeded(3500L));
        }
    }

    @Test
    public void determinesIfEagerRefreshWaitTimeExceeded() throws Exception {

        Clock clock = mock(Clock.class);
        when(clock.currentTimeMillis()).thenReturn(5000L);

        try (ConnectionAttemptManager connectionAttemptManager = new ConnectionAttemptManager(
                null,
                -1,
                1000,
                null,
                -1,
                clock)) {

            assertFalse(connectionAttemptManager.eagerRefreshWaitTimeExceeded(4500L));
            assertFalse(connectionAttemptManager.eagerRefreshWaitTimeExceeded(4000L));

            assertTrue(connectionAttemptManager.eagerRefreshWaitTimeExceeded(3500L));
        }
        ;

    }

    @Test
    public void alwaysReturnsFalseIfEagerRefreshWaitTimeNotSet() throws Exception {

        Clock clock = mock(Clock.class);
        when(clock.currentTimeMillis()).thenReturn(5000L);

        try (ConnectionAttemptManager connectionAttemptManager = new ConnectionAttemptManager(
                null,
                -1,
                -1,
                null,
                -1,
                clock)) {

            assertFalse(connectionAttemptManager.eagerRefreshWaitTimeExceeded(4500L));
            assertFalse(connectionAttemptManager.eagerRefreshWaitTimeExceeded(4000L));
            assertFalse(connectionAttemptManager.eagerRefreshWaitTimeExceeded(3500L));
        }
    }

    @Test
    public void shouldDoNothingIfOnEagerRefreshIsNull() throws Exception {

        ExecutorService executorService = mock(ExecutorService.class);

        try (ConnectionAttemptManager connectionAttemptManager = new ConnectionAttemptManager(
                null,
                -1,
                -1,
                null,
                -1,
                null,
                executorService,
                0,
                false)) {

            connectionAttemptManager.triggerEagerRefresh(new EagerRefreshContext());

            verify(executorService, never()).submit(any(Callable.class));
        }
    }

    @Test
    public void shouldSubmitRefreshEventTask() throws Exception {

        ExecutorService executorService = mock(ExecutorService.class);

        try (ConnectionAttemptManager connectionAttemptManager = new ConnectionAttemptManager(
                null,
                -1,
                -1,
                context -> null,
                -1,
                null,
                executorService,
                0,
                false)) {

            connectionAttemptManager.triggerEagerRefresh(new EagerRefreshContext());

            verify(executorService, times(1)).submit(any(ConnectionAttemptManager.RefreshEventTask.class));
        }
    }

    @Test
    public void shouldNotSubmitRefreshEventTaskIfWithinBackoffPeriod() throws Exception {

        Clock clock = mock(Clock.class);
        when(clock.currentTimeMillis()).thenReturn(5000L);

        ExecutorService executorService = mock(ExecutorService.class);

        try (ConnectionAttemptManager connectionAttemptManager = new ConnectionAttemptManager(
                null,
                -1,
                -1,
                context -> null,
                1000,
                clock,
                executorService,
                4500,
                false)) {

            connectionAttemptManager.triggerEagerRefresh(new EagerRefreshContext());

            verify(executorService, never()).submit(any(Callable.class));
        }
    }

    @Test
    public void shouldSubmitRefreshEventTaskIfOutsideBackoffPeriod() throws Exception {

        Clock clock = mock(Clock.class);
        when(clock.currentTimeMillis()).thenReturn(5000L);

        ExecutorService executorService = mock(ExecutorService.class);

        try (ConnectionAttemptManager connectionAttemptManager = new ConnectionAttemptManager(
                null,
                -1,
                -1,
                context -> null,
                1000,
                clock,
                executorService,
                3000,
                false)) {

            connectionAttemptManager.triggerEagerRefresh(new EagerRefreshContext());

            verify(executorService, times(1)).submit(any(ConnectionAttemptManager.RefreshEventTask.class));
        }
    }

    @Test
    public void shouldDoNothingIfAlreadyRefreshing() throws Exception {

        ExecutorService executorService = mock(ExecutorService.class);

        try (ConnectionAttemptManager connectionAttemptManager = new ConnectionAttemptManager(
                null,
                -1,
                -1,
                context -> null,
                -1,
                null,
                executorService,
                0,
                true)) {

            connectionAttemptManager.triggerEagerRefresh(new EagerRefreshContext());

            verify(executorService, never()).submit(any(Callable.class));
        }
    }

}