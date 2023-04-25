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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RefreshEventTaskTest {
    @Test
    public void shouldRefreshClient(){

        EndpointCollection endpointCollection = new EndpointCollection();

        EagerRefreshContext context = new EagerRefreshContext();
        AtomicLong latestRefreshTime = new AtomicLong(0);
        AtomicBoolean refreshing = new AtomicBoolean(false);

        Clock clock = mock(Clock.class);
        when(clock.currentTimeMillis()).thenReturn(5000L);

        Refreshable client = mock(Refreshable.class);
        OnEagerRefresh onEagerRefresh = mock(OnEagerRefresh.class);

        when(onEagerRefresh.getEndpoints(context)).thenReturn(endpointCollection);


        ConnectionAttemptManager.RefreshEventTask refreshEventTask =
                new ConnectionAttemptManager.RefreshEventTask(
                        context,
                        client,
                        refreshing,
                        latestRefreshTime,
                        onEagerRefresh,
                        clock);

        refreshEventTask.run();

        verify(client, times(1)).refreshEndpoints(endpointCollection);

        assertEquals(5000, latestRefreshTime.get());
        assertFalse(refreshing.get());
    }

    @Test
    public void shouldNotRefreshClientIfAlreadyRefreshing(){

        EndpointCollection endpointCollection = new EndpointCollection();

        EagerRefreshContext context = new EagerRefreshContext();
        AtomicLong latestRefreshTime = new AtomicLong(0);
        AtomicBoolean refreshing = new AtomicBoolean(true);

        Clock clock = mock(Clock.class);
        when(clock.currentTimeMillis()).thenReturn(5000L);

        Refreshable client = mock(Refreshable.class);
        OnEagerRefresh onEagerRefresh = mock(OnEagerRefresh.class);

        when(onEagerRefresh.getEndpoints(context)).thenReturn(endpointCollection);


        ConnectionAttemptManager.RefreshEventTask refreshEventTask =
                new ConnectionAttemptManager.RefreshEventTask(
                        context,
                        client,
                        refreshing,
                        latestRefreshTime,
                        onEagerRefresh,
                        clock);

        refreshEventTask.run();

        verify(client, never()).refreshEndpoints(endpointCollection);

        assertEquals(0, latestRefreshTime.get());
        assertTrue(refreshing.get());
    }
}
