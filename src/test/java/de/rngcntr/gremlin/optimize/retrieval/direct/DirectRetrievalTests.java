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

package de.rngcntr.gremlin.optimize.retrieval.direct;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.testutils.structure.MockedElementUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DirectRetrievalTests {

    @Test
    public void testConstructor() {
        DirectRetrieval<?> r = Mockito.mock(DirectRetrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        assertEquals(Retrieval.IMPOSSIBLE, r.getEstimatedSize());
    }

    @ParameterizedTest
    @MethodSource("generateTestElements")
    @SuppressWarnings("unchecked")
    public <E extends Element> void testEstimation(PatternElement<E> element, StatisticsProvider stats,
                                                   double expectedEstimation) {
        DirectRetrieval<E> r = Mockito.mock(DirectRetrieval.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(r.getElement()).thenReturn(element);

        r.estimate(stats);
        double estimation = r.getEstimatedSize();

        assertEquals(expectedEstimation, estimation);
    }

    @SuppressWarnings("unchecked")
    private static Stream<Arguments> generateTestElements() {
        LabelFilter<Vertex> vertexLabelFilter = Mockito.mock(LabelFilter.class);
        LabelFilter<Edge> edgeLabelFilter = Mockito.mock(LabelFilter.class);
        PropertyFilter<Vertex> highSelectivityVertexProperty = Mockito.mock(PropertyFilter.class);
        PropertyFilter<Vertex> lowSelectivityVertexProperty = Mockito.mock(PropertyFilter.class);
        PropertyFilter<Edge> highSelectivityEdgeProperty = Mockito.mock(PropertyFilter.class);
        PropertyFilter<Edge> lowSelectivityEdgeProperty = Mockito.mock(PropertyFilter.class);

        StatisticsProvider stats = Mockito.mock(StatisticsProvider.class);

        Mockito.when(stats.totals(Vertex.class)).thenReturn(1000D);
        Mockito.when(stats.withLabel(vertexLabelFilter)).thenReturn(100D);
        Mockito.when(stats.withProperty(vertexLabelFilter, highSelectivityVertexProperty)).thenReturn(1D);
        Mockito.when(stats.withProperty(vertexLabelFilter, lowSelectivityVertexProperty)).thenReturn(10D);

        Mockito.when(stats.totals(Edge.class)).thenReturn(2000D);
        Mockito.when(stats.withLabel(edgeLabelFilter)).thenReturn(200D);
        Mockito.when(stats.withProperty(edgeLabelFilter, highSelectivityEdgeProperty)).thenReturn(2D);
        Mockito.when(stats.withProperty(edgeLabelFilter, lowSelectivityEdgeProperty)).thenReturn(20D);

        return Stream.of(
                Arguments.of(MockedElementUtils.mockVertex(null), stats, 1000D),
                Arguments.of(MockedElementUtils.mockEdge(null), stats, 2000D),
                Arguments.of(MockedElementUtils.mockVertex(vertexLabelFilter), stats, 100D),
                Arguments.of(MockedElementUtils.mockEdge(edgeLabelFilter), stats, 200D),
                Arguments.of(MockedElementUtils.mockVertex(null, lowSelectivityVertexProperty), stats, 1000D),
                Arguments.of(MockedElementUtils.mockEdge(null, lowSelectivityEdgeProperty), stats, 2000D),
                Arguments.of(MockedElementUtils.mockVertex(vertexLabelFilter, lowSelectivityVertexProperty), stats, 10D),
                Arguments.of(MockedElementUtils.mockEdge(edgeLabelFilter, lowSelectivityEdgeProperty), stats, 20D),
                Arguments.of(MockedElementUtils.mockVertex(vertexLabelFilter, lowSelectivityVertexProperty, highSelectivityVertexProperty), stats, 1D),
                Arguments.of(MockedElementUtils.mockEdge(edgeLabelFilter, lowSelectivityEdgeProperty, highSelectivityEdgeProperty), stats, 2D)
        );
    }
}