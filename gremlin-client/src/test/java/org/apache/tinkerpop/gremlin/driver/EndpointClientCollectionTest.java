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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class EndpointClientCollectionTest {
    @Test
    public void shouldIdentifySurvivingEndpointClients(){

        DatabaseEndpoint endpoint1 = new DatabaseEndpoint().withAddress("address1");
        DatabaseEndpoint endpoint2 = new DatabaseEndpoint().withAddress("address2");
        DatabaseEndpoint endpoint3 = new DatabaseEndpoint().withAddress("address3");

        EndpointClient endpointClient1 = new EndpointClient(endpoint1, mock(Client.class));
        EndpointClient endpointClient2 = new EndpointClient(endpoint2, mock(Client.class));
        EndpointClient endpointClient3 = new EndpointClient(endpoint3, mock(Client.class));

        EndpointClientCollection endpointClientCollection =
                new EndpointClientCollection(
                        EndpointClientCollection.builder()
                                .withEndpointClients(Arrays.asList(endpointClient1, endpointClient2, endpointClient3)));

        List<EndpointClient> survivingEndpointClients =
                endpointClientCollection.getSurvivingEndpointClients(
                        new EndpointCollection(Arrays.asList(endpoint1, endpoint3)));

        assertEquals(2, survivingEndpointClients.size());
        assertTrue(survivingEndpointClients.contains(endpointClient1));
        assertTrue(survivingEndpointClients.contains(endpointClient3));
    }
}