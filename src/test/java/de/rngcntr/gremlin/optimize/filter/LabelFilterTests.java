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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LabelFilterTests {

    @ParameterizedTest
    @ValueSource(strings = {
            "testLabel0",
            "testLabel1"
    })
    public void testConstructor(String testLabel) {
        LabelFilter<?> filter = new LabelFilter<>(Vertex.class, testLabel);
        assertEquals(testLabel, filter.getLabel());
    }

    @Test
    public void testEmptyConstructor() {
        LabelFilter<?> filter = LabelFilter.empty(Vertex.class);
        assertNull(filter.getLabel());
    }

    @ParameterizedTest
    @MethodSource("provideSampleFiltersForEqualsCheck")
    public void testEquals(Class<? extends Element> classA, String labelA,
                           Class<? extends Element> classB, String labelB, boolean equal) {
        LabelFilter<?> a = new LabelFilter<>(classA, labelA);
        LabelFilter<?> b = new LabelFilter<>(classB, labelB);

        assertEquals(a, a);
        assertEquals(b, b);

        assertEquals(equal, a.equals(b));
        assertEquals(equal, b.equals(a));
    }

    private static Stream<Arguments> provideSampleFiltersForEqualsCheck() {
        return Stream.of(
                Arguments.of(Vertex.class, "A", Vertex.class, "A",  true),
                Arguments.of(Vertex.class, "A", Vertex.class, "B", false),
                Arguments.of(Vertex.class, "A",   Edge.class, "A", false),
                Arguments.of(Vertex.class, "A",   Edge.class, "B", false),
                Arguments.of(Vertex.class, "B", Vertex.class, "A", false),
                Arguments.of(Vertex.class, "B", Vertex.class, "B",  true),
                Arguments.of(Vertex.class, "B",   Edge.class, "A", false),
                Arguments.of(Vertex.class, "B",   Edge.class, "B", false),
                Arguments.of(  Edge.class, "A", Vertex.class, "A", false),
                Arguments.of(  Edge.class, "A", Vertex.class, "B", false),
                Arguments.of(  Edge.class, "A",   Edge.class, "A",  true),
                Arguments.of(  Edge.class, "A",   Edge.class, "B", false),
                Arguments.of(  Edge.class, "B", Vertex.class, "A", false),
                Arguments.of(  Edge.class, "B", Vertex.class, "B", false),
                Arguments.of(  Edge.class, "B",   Edge.class, "A", false),
                Arguments.of(  Edge.class, "B",   Edge.class, "B",  true)
        );
    }

    @ParameterizedTest
    @SuppressWarnings("unchecked")
    @ValueSource(classes = {
            Vertex.class,
            Edge.class
    })
    public <E extends Element> void testApplyTo(Class<E> clazz) {
        GraphTraversal<?,E> t = (GraphTraversal<?,E>) Mockito.mock(GraphTraversal.class);
        String testLabel = "testLabel";

        LabelFilter<E> testLabelFilter = new LabelFilter<>(clazz, testLabel);
        testLabelFilter.applyTo(t);

        Mockito.verify(t, Mockito.times(1)).hasLabel(testLabel);
        Mockito.verifyNoMoreInteractions(t);
    }
}