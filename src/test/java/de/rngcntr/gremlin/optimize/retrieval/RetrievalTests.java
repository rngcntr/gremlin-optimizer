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

package de.rngcntr.gremlin.optimize.retrieval;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RetrievalTests {

    @Test
    public void testConstructor() {
        Retrieval<?> r = Mockito.mock(Retrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        assertEquals(Retrieval.IMPOSSIBLE, r.getEstimatedSize());
    }

    @ParameterizedTest
    @CsvSource({
            "0, 0, 0",
            "0, 1, -1",
            "1, 0, 1",
            "1, 1, 0",
            "2, 1, 1",
            "1, 2, -1"
    })
    public void testCompareTo(double expectedA, double expectedB, int result) {
        Retrieval<?> a = Mockito.mock(Retrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(a.getEstimatedSize()).thenReturn(expectedA);
        Retrieval<?> b = Mockito.mock(Retrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(b.getEstimatedSize()).thenReturn(expectedB);

        int compareResult = a.compareTo(b);

        assertEquals(Math.signum(result), Math.signum(compareResult));
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @MethodSource("provideElementConstraints")
    public void testAsTraversal(String label, List<Pair<String, Object>> properties, long id) {
        LabelFilter<Vertex> labelFilter = label == null ? null : new LabelFilter<>(Vertex.class, label);
        List<PropertyFilter<Vertex>> propertyFilters = new ArrayList<>();
        properties.forEach(p -> propertyFilters.add(new PropertyFilter<>(Vertex.class, p.getValue0(), P.eq(p.getValue1()))));
        PatternElement<Vertex> element = Mockito.mock(PatternVertex.class);
        Mockito.when(element.hasLabelFilter()).thenReturn(label != null);
        Mockito.when(element.getLabelFilter()).thenReturn(labelFilter);
        Mockito.when(element.getPropertyFilters()).thenReturn(propertyFilters);
        Mockito.when(element.getId()).thenReturn(id);
        GraphTraversal<?,Vertex> t = Mockito.mock(GraphTraversal.class);
        Retrieval<Vertex> r = Mockito.mock(Retrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(r.getElement()).thenReturn(element);
        Mockito.doReturn(t).when(r).getBaseTraversal();

        r.asTraversal();

        Mockito.verify(t, Mockito.times(label == null ? 0 : 1)).hasLabel(label);
        for (PropertyFilter<Vertex> f : propertyFilters) {
            Mockito.verify(t, Mockito.atLeastOnce()).has(f.getKey(), f.getPredicate());
        }
        Mockito.verify(t, Mockito.times(1)).as(String.valueOf(id));
        Mockito.verifyNoMoreInteractions(t);
    }

    private static Stream<Arguments> provideElementConstraints() {
        List<Pair<String, Object>> properties = new ArrayList<>();
        properties.add(new Pair<>("key0", "value0"));
        properties.add(new Pair<>("key1", "value1"));
        properties.add(new Pair<>("key2", 1));

        return Stream.of(
                Arguments.of("label0", properties, 1L),
                Arguments.of(null, properties, 2L),
                Arguments.of(null, new ArrayList<>(), 1L),
                Arguments.of("label1", new ArrayList<>(), 2L)
        );
    }
}
