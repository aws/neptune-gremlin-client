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

package software.amazon.utils;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;

public class RetryUtils {
    public static Result isRetryableException(Exception ex) {
        Throwable e = ExceptionUtils.getRootCause(ex);

        Class<? extends Throwable> exceptionClass = e.getClass();

        String message = getMessage(e);

        if (NoHostAvailableException.class.isAssignableFrom(exceptionClass)) {
            return Result.RETRYABLE(e, message);
        }

        if (ConnectException.class.isAssignableFrom(exceptionClass)) {
            return Result.RETRYABLE(e, message);
        }

        // Check for connection issues
        if (message.contains("Timed out while waiting for an available host") ||
                message.contains("waiting for connection") ||
                message.contains("Connection to server is no longer active") ||
                message.contains("Connection reset by peer") ||
                message.contains("Connection refused") ||
                message.contains("SSLEngine closed already") ||
                message.contains("Pool is shutdown") ||
                message.contains("ExtendedClosedChannelException") ||
                message.contains("Broken pipe") ||
                message.contains("StacklessClosedChannelException") ) {
            return Result.RETRYABLE(e, message);
        }

        // Concurrent writes can sometimes trigger a ConcurrentModificationException.
        // In these circumstances you may want to backoff and retry.
        if (message.contains("ConcurrentModificationException")) {
            return Result.RETRYABLE(e, message);
        }

        // If the primary fails over to a new instance, existing connections to the old primary will
        // throw a ReadOnlyViolationException. You may want to back and retry.
        if (message.contains("ReadOnlyViolationException")) {
            return Result.RETRYABLE(e, message);
        }

        // CVEs can sometimes occur if a previous transaction is not yet visible to the current transaction.
        if (message.contains("ConstraintViolationException")) {
            return Result.RETRYABLE(e, message);
        }

        return Result.NOT_RETRYABLE(e, message);
    }

    private static String getMessage(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }

    public static class Result {

        private static Result RETRYABLE(Throwable e, String message){
            return new Result(true, e, message);
        }

        private static Result NOT_RETRYABLE(Throwable e, String message){
            return new Result(false, e, message);
        }

        private final boolean isRetryable;
        private final Throwable e;
        private final String message;

        public Result(boolean isRetryable, Throwable e, String message) {
            this.isRetryable = isRetryable;
            this.e = e;
            this.message = message;
        }

        public boolean isRetryable() {
            return isRetryable;
        }

        public Throwable rootCause() {
            return e;
        }

        public String message() {
            return message;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "isRetryable=" + isRetryable +
                    ", e=" + e +
                    ", message='" + message + '\'' +
                    '}';
        }
    }

}
