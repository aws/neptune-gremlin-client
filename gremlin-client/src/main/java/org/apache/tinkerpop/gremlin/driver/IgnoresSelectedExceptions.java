package org.apache.tinkerpop.gremlin.driver;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public interface IgnoresSelectedExceptions {

    public static Map<Class<? extends Exception>, Set<String>> getIgnorableExceptions() {
        Map<Class<? extends Exception>, Set<String>> ignorableExceptions = new HashMap<>();

        Set<String> ioExceptionSignatures = new HashSet<>();
        ioExceptionSignatures.add("Operation timed out");
        ignorableExceptions.put(IOException.class, ioExceptionSignatures);

        return ignorableExceptions;
    }
}