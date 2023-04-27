package software.amazon.utils;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CollectionUtilsTest {
    @Test
    public void shouldJoinTwoLists() {
        List<String> l1 = Arrays.asList("one", "two", "three");
        List<String> l2 = Arrays.asList("a", "b", "c");

        List<String> result = CollectionUtils.join(l1, l2);

        assertEquals(6, result.size());
        assertTrue(result.contains("one"));
        assertTrue(result.contains("two"));
        assertTrue(result.contains("three"));
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
    }

}