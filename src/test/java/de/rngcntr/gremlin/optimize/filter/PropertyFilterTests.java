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

package de.rngcntr.gremlin.optimize.filter;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PropertyFilterTests {

    @ParameterizedTest
    @CsvSource({
            "testKey0, 0",
            "testKey1, 1"
    })
    public void testConstructor(String key, int value) {
        P<?> p = P.eq(value);

        PropertyFilter<?> vertexFilter = new PropertyFilter<>(Vertex.class, key, p);
        PropertyFilter<?> edgeFilter = new PropertyFilter<>(Edge.class, key, p);

        assertEquals(key, vertexFilter.getKey());
        assertEquals(p, vertexFilter.getPredicate());
        assertEquals(Vertex.class, vertexFilter.getFilteredType());

        assertEquals(key, edgeFilter.getKey());
        assertEquals(p, edgeFilter.getPredicate());
        assertEquals(Edge.class, edgeFilter.getFilteredType());
    }

    @ParameterizedTest
    @MethodSource("provideSampleFiltersForEqualsCheck")
    public void testEquals(PropertyFilter<?> a, PropertyFilter<?> b, boolean equal) {
        assertEquals(a, a);
        assertEquals(b, b);

        assertEquals(equal, a.equals(b));
        assertEquals(equal, b.equals(a));
    }

    private static Stream<Arguments> provideSampleFiltersForEqualsCheck() {
        List<Arguments> arguments = new ArrayList<>();

        List<Class<? extends Element>> classes = Stream.of(Vertex.class, Edge.class).collect(Collectors.toList());
        List<String> keys = Arrays.asList("A", "B");
        List<Object> values = Arrays.asList("1", 1);
        List<Function<Object, P<Object>>> predicates = Arrays.asList(P::eq, P::neq);

        List<PropertyFilter<?>> filtersA = new ArrayList<>();
        List<PropertyFilter<?>> filtersB = new ArrayList<>();

        for (Class<? extends Element> clazz : classes) {
            for (String key : keys) {
                for (Function<Object, P<Object>> predicate : predicates) {
                    for (Object value : values) {
                        filtersA.add(new PropertyFilter<>(clazz, key, predicate.apply(value)));
                        filtersB.add(new PropertyFilter<>(clazz, key, predicate.apply(value)));
                    }
                }
            }
        }

        for (int i = 0; i < filtersA.size(); i++) {
            for (int j = 0; j < filtersB.size(); j++) {
                arguments.add(Arguments.of(filtersA.get(i), filtersB.get(j), i == j));
            }
        }

        return arguments.stream();
    }

    @ParameterizedTest
    @SuppressWarnings("unchecked")
    @ValueSource(classes = {
            Vertex.class,
            Edge.class
    })
    public <E extends Element> void testApplyTo(Class<E> clazz) {
        GraphTraversal<?,E> t = (GraphTraversal<?,E>) Mockito.mock(GraphTraversal.class);
        String testKey = "testKey";
        P<String> testPredicate = P.eq("testValue");

        PropertyFilter<E> testPropertyFilter = new PropertyFilter<>(clazz, testKey, testPredicate);
        testPropertyFilter.applyTo(t);

        Mockito.verify(t, Mockito.times(1)).has(testKey, testPredicate);
        Mockito.verifyNoMoreInteractions(t);
    }
}