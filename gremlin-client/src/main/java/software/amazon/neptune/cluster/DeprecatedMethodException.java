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

public class DeprecatedMethodException extends RuntimeException {
    public DeprecatedMethodException(Class<?> clazz){
        super(String.format("Tried to call a deprecated method on the interface %s for which there is no implementation", clazz.getSimpleName()));
    }
}
