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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.driver.ApprovalResult.REJECTED_REASON_ANNOTATION;

public class ClientHolderCollection implements Iterable<ClientHolder> {

    private final List<ClientHolder> clients = new ArrayList<>();
    private final EndpointCollection acceptedEndpoints;
    private final EndpointCollection rejectedEndpoints;

    public ClientHolderCollection(){
        this(new EndpointCollection(), new EndpointCollection());
    }

    public ClientHolderCollection(EndpointCollection acceptedEndpoints) {
        this(acceptedEndpoints, new EndpointCollection());
    }

    public ClientHolderCollection(EndpointCollection acceptedEndpoints,
                                  EndpointCollection rejectedEndpoints) {
        this.acceptedEndpoints = acceptedEndpoints;
        this.rejectedEndpoints = rejectedEndpoints;
    }

    public void add(ClientHolder clientHolder){
        clients.add(clientHolder);
    }

    public ClientHolder get(int index){
        return clients.get(index);
    }

    public int size(){
        return clients.size();
    }

    public boolean isEmpty() {
        return clients.isEmpty();
    }

    @Override
    public Iterator<ClientHolder> iterator() {
        return clients.iterator();
    }

    public Stream<ClientHolder> stream(){
        return clients.stream();
    }

    public boolean hasRejectedEndpoints() {
        return !rejectedEndpoints.isEmpty();
    }

    public Collection<String> rejectionReasons() {
        return rejectedEndpoints.endpoints().stream()
                .map(e -> e.getAnnotations().getOrDefault(REJECTED_REASON_ANNOTATION, "unknown"))
                .collect(Collectors.toSet());
    }
}
