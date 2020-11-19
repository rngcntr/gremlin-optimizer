// Copyright 2020 Florian Grieskamp
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package de.rngcntr.gremlin.optimize.step;

import de.rngcntr.gremlin.optimize.query.JoinAttribute;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
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
                        makeMap("0", 0),
                        Collections.singletonList(makeMap("0", 0)),
                        Collections.singletonList("0"),
                        Collections.singletonList(makeMap("0", 0))
                ),
                Arguments.of(
                        makeMap("0", 0),
                        Collections.singletonList(makeMap("1", 0)),
                        Collections.emptyList(),
                        Collections.singletonList(makeMap("0", 0, "1", 0))
                ),
                Arguments.of(
                        makeMap("0", 0),
                        Collections.singletonList(makeMap("0", 1)),
                        Collections.singletonList("0"),
                        Collections.emptyList()
                ),
                Arguments.of(
                        makeMap("0", 0),
                        Collections.singletonList(makeMap("0", 1, "1", 2)),
                        Collections.singletonList("0"),
                        Collections.emptyList()
                ),
                Arguments.of(
                        makeMap("0", 0, "1", 2),
                        Collections.singletonList(makeMap("0", 1, "1", 2)),
                        Arrays.asList("0", "1"),
                        Collections.emptyList()
                ),
                Arguments.of(
                        makeMap("0", 0),
                        Arrays.asList(makeMap("0", 0, "1", 2), makeMap("0", 1, "1", 3)),
                        Collections.singletonList("0"),
                        Collections.singletonList(makeMap("0", 0, "1", 2))
                ),
                Arguments.of(
                        makeMap("0", 0, "2", 4),
                        Arrays.asList(makeMap("0", 0, "1", 2), makeMap("0", 1, "1", 3)),
                        Arrays.asList("0"),
                        Collections.singletonList(makeMap("0", 0, "1", 2, "2", 4))
                ),
                Arguments.of(
                        makeMap("0", 0, "2", 4),
                        Arrays.asList(makeMap("0", 0, "1", 2), makeMap("0", 0, "1", 3)),
                        Arrays.asList("0"),
                        Arrays.asList(
                                makeMap("0", 0, "1", 2, "2", 4),
                                makeMap("0", 0, "1", 3, "2", 4)
                        )
                )
        );
    }

    @ParameterizedTest
    @Disabled // TODO enable
    @MethodSource("generateArguments")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testFlatMap(Map<String, Object> traverserContent, List<Map<String, Object>> joinTuples,
                            List<String> joinAttributes, List<Map<String, Object>> expectedResults) {
        Graph graph = Mockito.mock(Graph.class);
        Traversal.Admin parentTraversal = Mockito.mock(Traversal.Admin.class);
        Traversal innerTraversal = Mockito.mock(Traversal.class);
        Traversal.Admin innerTraversalAdmin = Mockito.mock(Traversal.Admin.class);
        Traverser.Admin traverser = Mockito.mock(Traverser.Admin.class);
        Path path = ImmutablePath.make();

        Mockito.when(parentTraversal.getGraph()).thenReturn(Optional.of(graph));
        Mockito.when(innerTraversal.asAdmin()).thenReturn(innerTraversalAdmin);
        Mockito.when(innerTraversalAdmin.toList()).thenReturn(joinTuples);
        Mockito.when(traverser.get()).thenReturn(traverserContent);
        Mockito.when(traverser.path()).thenReturn(path);
        Mockito.when(traverser.bulk()).thenReturn(1L); // TODO integrate into test cases
        Mockito.when(innerTraversalAdmin.getSideEffects()).thenReturn(new DefaultTraversalSideEffects());
        Mockito.when(innerTraversalAdmin.clone()).thenReturn(innerTraversalAdmin);

        JoinStep js = new JoinStep(parentTraversal, innerTraversal, makeJoinAttributes(joinAttributes));
        Iterator result = js.flatMap(traverser);

        assertEqualsIterators(expectedResults.iterator(), result);
        Mockito.verify(innerTraversalAdmin, Mockito.times(1)).toList();
        assertEquals(1, js.getLocalChildren().size());
        assertEquals(innerTraversalAdmin, js.getLocalChildren().get(0));
    }

    private Set<JoinAttribute> makeJoinAttributes(Collection<String> joinAttributes) {
        HashSet<JoinAttribute> s = new HashSet<>();
        joinAttributes.forEach(idString -> {
            PatternElement<?> pe = Mockito.mock(PatternElement.class);
            Mockito.when(pe.getId()).thenReturn(Long.parseLong(idString));
            s.add(new JoinAttribute(pe));
        });
        return s;
    }

    @Test
    public void testClone() {
        Traversal.Admin<?,?> mockedParentTraversal = Mockito.mock(Traversal.Admin.class);
        Traversal.Admin<Map<String, Object>, Map<String, Object>> mockedMatchTraversal = Mockito.mock(Traversal.Admin.class);
        Traversal.Admin<Map<String, Object>, Map<String, Object>> clonedMatchTraversal = Mockito.mock(Traversal.Admin.class);
        Mockito.when(mockedMatchTraversal.asAdmin()).thenReturn(mockedMatchTraversal);
        Mockito.when(mockedMatchTraversal.getSideEffects()).thenReturn(new DefaultTraversalSideEffects());
        Mockito.when(mockedMatchTraversal.clone()).thenReturn(clonedMatchTraversal);

        JoinStep js = new JoinStep(mockedParentTraversal, mockedMatchTraversal, Collections.emptySet());
        JoinStep clone = js.clone();

        assertEquals(1, clone.getLocalChildren().size());
        assertEquals(clonedMatchTraversal, clone.getLocalChildren().get(0));
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
