package de.rngcntr.gremlin.optimize.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class JoinStepTests {

    private static Stream<Arguments> generateArguments() {
        return Stream.of(
                Arguments.of(
                        makeMap("a", 0),
                        Collections.singletonList(makeMap("a", 0)),
                        Collections.singletonList(makeMap("a", 0))
                ),
                Arguments.of(
                        makeMap("a", 0),
                        Collections.singletonList(makeMap("b", 0)),
                        Collections.singletonList(makeMap("a", 0, "b", 0))
                ),
                Arguments.of(
                        makeMap("a", 0),
                        Collections.singletonList(makeMap("a", 1)),
                        Collections.emptyList()
                ),
                Arguments.of(
                        makeMap("a", 0),
                        Collections.singletonList(makeMap("a", 1, "b", 2)),
                        Collections.emptyList()
                ),
                Arguments.of(
                        makeMap("a", 0, "b", 2),
                        Collections.singletonList(makeMap("a", 1, "b", 2)),
                        Collections.emptyList()
                ),
                Arguments.of(
                        makeMap("a", 0),
                        Arrays.asList(makeMap("a", 0, "b", 2), makeMap("a", 1, "b", 3)),
                        Collections.singletonList(makeMap("a", 0, "b", 2))
                ),
                Arguments.of(
                        makeMap("a", 0, "c", 4),
                        Arrays.asList(makeMap("a", 0, "b", 2), makeMap("a", 1, "b", 3)),
                        Collections.singletonList(makeMap("a", 0, "b", 2, "c", 4))
                ),
                Arguments.of(
                        makeMap("a", 0, "c", 4),
                        Arrays.asList(makeMap("a", 0, "b", 2), makeMap("a", 0, "b", 3)),
                        Arrays.asList(
                                makeMap("a", 0, "b", 2, "c", 4),
                                makeMap("a", 0, "b", 3, "c", 4)
                        )
                )
        );
    }

    @ParameterizedTest
    @MethodSource("generateArguments")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testFlatMap(Map<String, Object> traverserContent, List<Map<String, Object>> joinTuples,
                            List<Map<String, Object>> expectedResults) {
        Graph graph = Mockito.mock(Graph.class);
        Traversal.Admin parentTraversal = Mockito.mock(Traversal.Admin.class);
        Traversal innerTraversal = Mockito.mock(Traversal.class);
        Traversal.Admin innerTraversalAdmin = Mockito.mock(Traversal.Admin.class);
        Traverser.Admin traverser = Mockito.mock(Traverser.Admin.class);

        Mockito.when(parentTraversal.getGraph()).thenReturn(Optional.of(graph));
        Mockito.when(innerTraversal.asAdmin()).thenReturn(innerTraversalAdmin);
        Mockito.when(innerTraversal.toList()).thenReturn(joinTuples);
        Mockito.when(traverser.get()).thenReturn(traverserContent);
        Mockito.when(innerTraversalAdmin.getSideEffects()).thenReturn(new DefaultTraversalSideEffects());

        JoinStep js = new JoinStep(parentTraversal, innerTraversal);
        Iterator result = js.flatMap(traverser);

        assertEqualsIterators(expectedResults.iterator(), result);
        Mockito.verify(innerTraversalAdmin).setGraph(graph);
        Mockito.verify(innerTraversal, Mockito.times(1)).toList();
    }

    private static Map<String, Object> makeMap(String k0, Object v0) {
        Map<String, Object> map = new HashMap<>();
        map.put(k0, v0);
        return map;
    }

    private static Map<String, Object> makeMap(String k0, Object v0, String k1, Object v1) {
        Map<String, Object> map = makeMap(k0, v0);
        map.put(k1, v1);
        return map;
    }

    private static Map<String, Object> makeMap(String k0, Object v0, String k1, Object v1, String k2, Object v2) {
        Map<String, Object> map = makeMap(k0, v0, k1, v1);
        map.put(k2, v2);
        return map;
    }

    private static void assertEqualsIterators(Iterator<?> a, Iterator<?> b) {
        while (a.hasNext() && b.hasNext()) {
            assertEquals(a.next(), b.next());
        }
        assertFalse(a.hasNext());
        assertFalse(b.hasNext());
    }
}
