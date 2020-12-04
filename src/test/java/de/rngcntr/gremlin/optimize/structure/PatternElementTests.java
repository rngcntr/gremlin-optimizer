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

package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class PatternElementTests {

    static Stream<Class<? extends Element>> classParams() {
        return Stream.of(Vertex.class, Edge.class);
    }

    @ParameterizedTest
    @MethodSource("classParams")
    public void testConstructor(Class<? extends Element> clazz) {
        PatternElement<?> element = Mockito.mock(PatternElement.class,
                Mockito.withSettings().useConstructor(clazz).defaultAnswer(Mockito.CALLS_REAL_METHODS));

        assertEquals(clazz, element.getType());
        assertFalse(element.hasLabelFilter());
        assertEquals(Collections.emptyList(), element.getPropertyFilters());
        assertEquals(Collections.emptyList(), element.getRetrievals());
    }

    @ParameterizedTest
    @MethodSource("classParams")
    public void testInitializeRetrievals(Class<? extends Element> clazz) {
        PatternElement<?> element = Mockito.mock(PatternElement.class,
                Mockito.withSettings().useConstructor(clazz).defaultAnswer(Mockito.CALLS_REAL_METHODS));

        element.initializeRetrievals();

        Mockito.verify(element, Mockito.times(1)).generateDirectRetrieval();
        Mockito.verify(element, Mockito.times(1)).generateDependentRetrievals();
    }

    @ParameterizedTest
    @MethodSource("classParams")
    @SuppressWarnings("rawtypes")
    public void testEstimateDirectRetrievals(Class<? extends Element> clazz) {
        PatternElement element = Mockito.mock(PatternElement.class,
                Mockito.withSettings().useConstructor(clazz).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Retrieval directRetrieval = Mockito.mock(DirectRetrieval.class);
        Retrieval dependentRetrieval = Mockito.mock(DependentRetrieval.class);
        StatisticsProvider stats = Mockito.mock(StatisticsProvider.class);
        Mockito.when(element.getRetrievals()).thenReturn(Arrays.asList(directRetrieval, dependentRetrieval));

        element.estimateDirectRetrievals(stats);

        Mockito.verify(directRetrieval, Mockito.times(1)).estimate(stats);
        Mockito.verify(dependentRetrieval, Mockito.times(0)).estimate(stats);
    }

    @ParameterizedTest
    @MethodSource("classParams")
    @SuppressWarnings("rawtypes")
    public void testEstimateDependentRetrievals(Class<? extends Element> clazz) {
        PatternElement element = Mockito.mock(PatternElement.class,
                Mockito.withSettings().useConstructor(clazz).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Retrieval directRetrieval = Mockito.mock(DirectRetrieval.class);
        Retrieval dependentRetrieval = Mockito.mock(DependentRetrieval.class);
        StatisticsProvider stats = Mockito.mock(StatisticsProvider.class);
        Mockito.when(element.getRetrievals()).thenReturn(Arrays.asList(directRetrieval, dependentRetrieval));

        element.estimateDependentRetrievals(stats);

        Mockito.verify(directRetrieval, Mockito.times(0)).estimate(stats);
        Mockito.verify(dependentRetrieval, Mockito.times(1)).estimate(stats);
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> testGetDependentRetrieval() {
        return classParams().flatMap(clazz -> {
            PatternElement<Vertex> v = makePatternElement(Vertex.class);
            PatternElement<Edge> e = makePatternElement(Edge.class);

            DependentRetrieval<?> ret0v = makeDependentRetrieval(v, v, 0);
            DependentRetrieval<?> ret1v = makeDependentRetrieval(v, v, 1);
            DependentRetrieval<?> ret2v = makeDependentRetrieval(v, v, 2);
            DependentRetrieval<?> ret0e = makeDependentRetrieval(v, e, 0);
            DependentRetrieval<?> ret1e = makeDependentRetrieval(v, e, 1);
            DependentRetrieval<?> ret2e = makeDependentRetrieval(v, e, 2);
            DirectRetrieval<?> retDirect0 = makeDirectRetrieval(v, 0);

            return Stream.of(
                    Arguments.of(clazz, Collections.emptyList(), v, null),
                    Arguments.of(clazz, Collections.emptyList(), e, null),
                    Arguments.of(clazz, Collections.singletonList(ret1v), v, ret1v),
                    Arguments.of(clazz, Collections.singletonList(ret1v), e, null),
                    Arguments.of(clazz, Collections.singletonList(retDirect0), e, null),
                    Arguments.of(clazz, Arrays.asList(ret2e, retDirect0), e, ret2e),
                    Arguments.of(clazz, Arrays.asList(ret2e, retDirect0), v, null),
                    Arguments.of(clazz, Arrays.asList(ret0e, ret0v), e, ret0e),
                    Arguments.of(clazz, Arrays.asList(ret0e, ret0v), v, ret0v),
                    Arguments.of(clazz, Arrays.asList(ret0e, ret1e), e, ret0e),
                    Arguments.of(clazz, Arrays.asList(ret0v, ret1e, ret2e), e, ret1e)
            );
        });
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("unchecked")
    public <E extends Element> void testGetDependentRetrieval(Class<E> clazz, List<Retrieval<E>> retrievals,
                                                                  PatternElement<E> sourceElement,
                                                                  DependentRetrieval<E> expectedResult) {
        PatternElement<E> element = (PatternElement<E>) Mockito.mock(PatternElement.class,
                Mockito.withSettings().useConstructor(clazz).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(element.getRetrievals()).thenReturn(retrievals);

        assertEquals(expectedResult == null ? Optional.empty() : Optional.of(expectedResult),
                element.getDependentRetrieval(sourceElement));
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> testGetBestDependentRetrieval() {
        return classParams().flatMap(clazz -> {
            PatternElement<Vertex> v = makePatternElement(Vertex.class);
            PatternElement<Edge> e = makePatternElement(Edge.class);

            DependentRetrieval<?> ret0 = makeDependentRetrieval(v, e, 0);
            DependentRetrieval<?> ret1 = makeDependentRetrieval(v, e, 1);
            DependentRetrieval<?> ret2 = makeDependentRetrieval(v, e, 2);
            DirectRetrieval<?> retDirect1 = makeDirectRetrieval(v, 1);

            return Stream.of(
                    Arguments.of(clazz, Collections.emptyList(), null),
                    Arguments.of(clazz, Collections.singletonList(ret1), ret1),
                    Arguments.of(clazz, Collections.singletonList(retDirect1), null),
                    Arguments.of(clazz, Collections.singletonList(ret2), ret2),
                    Arguments.of(clazz, Arrays.asList(ret2, retDirect1), ret2),
                    Arguments.of(clazz, Arrays.asList(ret0, ret2), ret0),
                    Arguments.of(clazz, Arrays.asList(ret1, ret0), ret0)
            );
        });
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("rawtypes")
    public <E extends Element> void testGetBestDependentRetrieval(Class<E> clazz, List<Retrieval<E>> retrievals,
                                                                  DependentRetrieval<E> expectedResult) {
        PatternElement element = Mockito.mock(PatternElement.class,
                Mockito.withSettings().useConstructor(clazz).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(element.getRetrievals()).thenReturn(retrievals);

        assertEquals(expectedResult == null ? Optional.empty() : Optional.of(expectedResult), element.getBestDependentRetrieval());
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> testGetBestRetrieval() {
        return classParams().flatMap(clazz -> {
            PatternElement<Vertex> v = makePatternElement(Vertex.class);
            PatternElement<Edge> e = makePatternElement(Edge.class);

            DependentRetrieval<?> ret0 = makeDependentRetrieval(v, e, 0);
            DependentRetrieval<?> ret1 = makeDependentRetrieval(v, e, 1);
            DependentRetrieval<?> ret2 = makeDependentRetrieval(v, e, 2);
            DirectRetrieval<?> retDirect0 = makeDirectRetrieval(v, 0);

            return Stream.of(
                    Arguments.of(clazz, Collections.emptyList(), null),
                    Arguments.of(clazz, Collections.singletonList(ret1), ret1),
                    Arguments.of(clazz, Collections.singletonList(retDirect0), retDirect0),
                    Arguments.of(clazz, Collections.singletonList(ret2), ret2),
                    Arguments.of(clazz, Arrays.asList(ret2, retDirect0), retDirect0),
                    Arguments.of(clazz, Arrays.asList(ret0, ret2), ret0),
                    Arguments.of(clazz, Arrays.asList(ret1, ret0), ret0)
            );
        });
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings("rawtypes")
    public <E extends Element> void testGetBestRetrieval(Class<E> clazz, List<Retrieval<E>> retrievals,
                                                         Retrieval<E> expectedResult) {
        PatternElement element = Mockito.mock(PatternElement.class,
                Mockito.withSettings().useConstructor(clazz).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(element.getRetrievals()).thenReturn(retrievals);

        if (expectedResult == null) {
            assertThrows(NoSuchElementException.class, element::getBestRetrieval);
        } else {
            assertEquals(expectedResult, element.getBestRetrieval());
        }
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> testEquals() {
        List<Class<? extends Element>> classes = classParams().collect(Collectors.toList());
        List<String> labels = Arrays.asList(null, "A", "B");
        List<List<Pair<String, Object>>> properties = Arrays.asList(
                Collections.emptyList(),
                Collections.singletonList(new Pair<>("a", 0)),
                Collections.singletonList(new Pair<>("b", 0)),
                Collections.singletonList(new Pair<>("a", 1)),
                Collections.singletonList(new Pair<>("b", 1)),
                Arrays.asList(new Pair<>("a", 0), new Pair<>("b", 1))
        );

        List<PatternElement<?>> elements1 = classes.stream()
                .flatMap(clazz -> labels.stream()
                        .flatMap(label -> properties.stream()
                                .flatMap(props ->
                                        Stream.of(makePatternElement(clazz, label, props))
        ))).collect(Collectors.toList());

        List<PatternElement<?>> elements2 = classes.stream()
                .flatMap(clazz -> labels.stream()
                        .flatMap(label -> properties.stream()
                                .flatMap(props ->
                                        Stream.of(makePatternElement(clazz, label, props))
        ))).collect(Collectors.toList());

        List<Arguments> arguments = new ArrayList<>();

        for (int i = 0; i < elements1.size(); i++) {
            for (int j = 0; j < elements2.size(); j++) {
                arguments.add(Arguments.of(elements1.get(i), elements2.get(j), i == j));
            }
            arguments.add(Arguments.of(elements1.get(i), new Object(), false));
        }

        return arguments.stream();
    }

    @ParameterizedTest
    @MethodSource
    public void testEquals(PatternElement<?> a, Object b, boolean expectedResult) {
        assertEquals(expectedResult, a.equals(b));
    }

    @SuppressWarnings("unused")
    private static Stream<Arguments> testIsIsomorphic() {
        PatternElement<Edge> e1 = makePatternElement(Edge.class);
        PatternElement<Edge> e2 = makePatternElement(Edge.class);
        PatternElement<Edge> em1 = makePatternElement(Edge.class);
        PatternElement<Edge> em2 = makePatternElement(Edge.class);
        PatternElement<Vertex> v1 = withNeighbors(makePatternElement(Vertex.class), Direction.IN, Collections.emptyList());
        PatternElement<Vertex> v2 = withNeighbors(makePatternElement(Vertex.class), Direction.IN, Collections.singletonList(e1));
        PatternElement<Vertex> v3 = withNeighbors(makePatternElement(Vertex.class), Direction.IN, Collections.singletonList(e1));
        PatternElement<Vertex> v4 = withNeighbors(makePatternElement(Vertex.class), Direction.OUT, Collections.singletonList(e1));
        PatternElement<Vertex> v5 = withNeighbors(makePatternElement(Vertex.class), Direction.IN, Arrays.asList(e1, e2));
        PatternElement<Vertex> vm1 = withNeighbors(makePatternElement(Vertex.class), Direction.IN, Collections.emptyList());
        PatternElement<Vertex> vm2 = withNeighbors(makePatternElement(Vertex.class), Direction.IN, Collections.singletonList(em1));
        PatternElement<Vertex> vm3 = withNeighbors(makePatternElement(Vertex.class), Direction.IN, Collections.singletonList(em1));
        PatternElement<Vertex> vm4 = withNeighbors(makePatternElement(Vertex.class), Direction.OUT, Collections.singletonList(em1));
        PatternElement<Vertex> vm5 = withNeighbors(makePatternElement(Vertex.class), Direction.IN, Arrays.asList(em1, em2));

        Map<PatternElement<?>, PatternElement<?>> elementMap = new HashMap<>();
        elementMap.put(v1, vm1);
        elementMap.put(v2, vm2);
        elementMap.put(e1, em1);
        elementMap.put(e2, em2);

        return Stream.of(
                Arguments.of(v1, vm1, elementMap, true),
                Arguments.of(v1, vm2, elementMap, false),
                Arguments.of(v1, vm3, elementMap, false),
                Arguments.of(v1, vm4, elementMap, false),
                Arguments.of(v1, vm5, elementMap, false),
                Arguments.of(v2, vm1, elementMap, false),
                Arguments.of(v2, vm2, elementMap, true),
                Arguments.of(v2, vm3, elementMap, true),
                Arguments.of(v2, vm4, elementMap, false),
                Arguments.of(v2, vm5, elementMap, false),
                Arguments.of(v3, vm1, elementMap, false),
                Arguments.of(v3, vm2, elementMap, true),
                Arguments.of(v3, vm3, elementMap, true),
                Arguments.of(v3, vm4, elementMap, false),
                Arguments.of(v3, vm5, elementMap, false),
                Arguments.of(v4, vm1, elementMap, false),
                Arguments.of(v4, vm2, elementMap, false),
                Arguments.of(v4, vm3, elementMap, false),
                Arguments.of(v4, vm4, elementMap, true),
                Arguments.of(v4, vm5, elementMap, false),
                Arguments.of(v5, vm1, elementMap, false),
                Arguments.of(v5, vm2, elementMap, false),
                Arguments.of(v5, vm3, elementMap, false),
                Arguments.of(v5, vm4, elementMap, false),
                Arguments.of(v5, vm5, elementMap, true)
        );
    }

    @ParameterizedTest
    @MethodSource
    public void testIsIsomorphic(PatternElement<?> self, PatternElement<?> other,
                                 Map<PatternElement<?>, PatternElement<?>> map, boolean expectedResult) {
        assertEquals(expectedResult, self.isIsomorphicTo(other, map));
    }

    @SuppressWarnings({"unused", "unchecked"})
    private static Stream<Arguments> testGetDependentNeighbors() {
        PatternElement<Vertex> source = Mockito.mock(PatternElement.class, Mockito.withSettings().useConstructor(Vertex.class).defaultAnswer(Mockito.CALLS_REAL_METHODS));

        PatternElement<Edge> e1 = Mockito.mock(PatternElement.class, Mockito.withSettings().useConstructor(Edge.class).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        PatternElement<Edge> e2 = Mockito.mock(PatternElement.class, Mockito.withSettings().useConstructor(Edge.class).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        PatternElement<Edge> e3 = Mockito.mock(PatternElement.class, Mockito.withSettings().useConstructor(Edge.class).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        PatternElement<Edge> e4 = Mockito.mock(PatternElement.class, Mockito.withSettings().useConstructor(Edge.class).defaultAnswer(Mockito.CALLS_REAL_METHODS));

        DependentRetrieval<Edge> r11 = makeDependentRetrieval(e1, source, 1L);
        DependentRetrieval<Edge> r21 = makeDependentRetrieval(e2, source, 2L);
        DependentRetrieval<Edge> r22 = makeDependentRetrieval(e2, source, 3L);
        DependentRetrieval<Edge> r31 = makeDependentRetrieval(e3, source, 3L);
        DependentRetrieval<Edge> r32 = makeDependentRetrieval(e3, e2, 2L);
        DependentRetrieval<Edge> r41 = makeDependentRetrieval(e4, e2, 2L);

        Mockito.when(e1.getRetrievals()).thenReturn(Collections.singletonList(r11));
        Mockito.when(e2.getRetrievals()).thenReturn(Arrays.asList(r21, r22));
        Mockito.when(e3.getRetrievals()).thenReturn(Arrays.asList(r31, r32));
        Mockito.when(e4.getRetrievals()).thenReturn(Collections.singletonList(r41));

        return Stream.of(
                Arguments.of(source, Collections.singletonList(e1), Collections.singletonList(e1)),
                Arguments.of(source, Collections.singletonList(e2), Collections.singletonList(e2)),
                Arguments.of(source, Collections.singletonList(e3), Collections.emptyList()),
                Arguments.of(source, Collections.singletonList(e4), Collections.emptyList()),
                Arguments.of(source, Arrays.asList(e4, e4), Collections.emptyList()),
                Arguments.of(source, Arrays.asList(e1, e2, e3), Arrays.asList(e1, e2)),
                Arguments.of(source, Arrays.asList(e1, e2, e3, e4), Arrays.asList(e1, e2))
        );
    }

    @ParameterizedTest
    @MethodSource
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testGetDependentNeighbors(PatternElement element, List<PatternElement<?>> allNeighbors, List<PatternElement<?>> expectedNeighbors) {
        withNeighbors(element, Direction.BOTH, allNeighbors);
        assertEquals(new HashSet<>(expectedNeighbors), new HashSet<>(element.getDependentNeighbors()));

    }

    @SuppressWarnings("unchecked")
    private static <E extends Element> PatternElement<E> makePatternElement(Class<E> type) {
        PatternElement<E> elem = Mockito.mock(PatternElement.class, Mockito.withSettings().useConstructor(type)
                .defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(elem.getType()).thenReturn(type);
        Mockito.when(elem.hasLabelFilter()).thenReturn(false);
        Mockito.when(elem.getPropertyFilters()).thenReturn(Collections.emptyList());
        return elem;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <E extends Element> PatternElement<E> makePatternElement(Class<E> type, String label,
                                                                            List<Pair<String, Object>> properties) {
        PatternElement elem = type == Vertex.class ? new PatternVertex() : new PatternEdge();
        if (label != null) {
            LabelFilter lFilter = new LabelFilter(type, label);
            elem.setLabelFilter(lFilter);
        }
        if (properties != null) {
            properties.forEach(e -> elem.addPropertyFilter(new PropertyFilter(type, e.getValue0(), P.eq(e.getValue1()))));
        }
        return elem;
    }

    private static <E extends Element> PatternElement<E> withNeighbors(PatternElement<E> element, Direction direction,
                                                                       List<PatternElement<?>> neighbors) {
        Mockito.when(element.getNeighbors(direction)).thenReturn(neighbors);
        return element;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Element> DirectRetrieval<E> makeDirectRetrieval(PatternElement<E> elem, double estimation) {
        DirectRetrieval<E> ret = Mockito.mock(DirectRetrieval.class, Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(ret.getEstimatedSize()).thenReturn(estimation);
        Mockito.when(ret.getElement()).thenReturn(elem);
        return ret;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <E extends Element> DependentRetrieval<E> makeDependentRetrieval(PatternElement<E> elem, PatternElement<?> source, double estimation) {
        DependentRetrieval<E> ret = Mockito.mock(DependentRetrieval.class, Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(ret.getEstimatedSize()).thenReturn(estimation);
        Mockito.when(ret.getSource()).thenReturn((PatternElement) source);
        Mockito.when(ret.getElement()).thenReturn(elem);
        return ret;
    }
}