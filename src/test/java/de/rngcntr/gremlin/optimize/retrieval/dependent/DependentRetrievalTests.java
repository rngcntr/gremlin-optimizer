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

package de.rngcntr.gremlin.optimize.retrieval.dependent;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import de.rngcntr.gremlin.optimize.testutils.structure.MockedElementUtils;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class DependentRetrievalTests {

    @SuppressWarnings("unchecked")
    private static Stream<Arguments> generateTestElements() {
        LabelFilter<Vertex> vertexLabelFilter = Mockito.mock(LabelFilter.class);
        LabelFilter<Edge> edgeLabelFilter = Mockito.mock(LabelFilter.class);

        PropertyFilter<Vertex> highSelectivityVertexProperty = Mockito.mock(PropertyFilter.class);
        PropertyFilter<Vertex> lowSelectivityVertexProperty = Mockito.mock(PropertyFilter.class);
        PropertyFilter<Edge> highSelectivityEdgeProperty = Mockito.mock(PropertyFilter.class);
        PropertyFilter<Edge> lowSelectivityEdgeProperty = Mockito.mock(PropertyFilter.class);

        StatisticsProvider stats = Mockito.mock(StatisticsProvider.class);

        long vTotal = 10_000L, vLabel = 1_000L, vHighSel = 10L, vLowSel = 100L;
        Mockito.when(stats.totals(Vertex.class)).thenReturn(vTotal);
        Mockito.when(stats.withLabel(vertexLabelFilter)).thenReturn(vLabel);
        Mockito.when(stats.withProperty(vertexLabelFilter, highSelectivityVertexProperty)).thenReturn(vHighSel);
        Mockito.when(stats.withProperty(vertexLabelFilter, lowSelectivityVertexProperty)).thenReturn(vLowSel);

        long eTotal = 20_000L, eLabel = 2_000L, eHighSel = 20L, eLowSel = 200L;
        Mockito.when(stats.totals(Edge.class)).thenReturn(eTotal);
        Mockito.when(stats.withLabel(edgeLabelFilter)).thenReturn(eLabel);
        Mockito.when(stats.withProperty(edgeLabelFilter, highSelectivityEdgeProperty)).thenReturn(eHighSel);
        Mockito.when(stats.withProperty(edgeLabelFilter, lowSelectivityEdgeProperty)).thenReturn(eLowSel);

        long vL2eL = 300L, eL2vL = 400L;
        Mockito.when(stats.connections(vertexLabelFilter, edgeLabelFilter)).thenReturn(vL2eL);
        Mockito.when(stats.connections(edgeLabelFilter, vertexLabelFilter)).thenReturn(eL2vL);

        long vL2e = 3_000L, eL2v = 4_000L, e2vL = 5_000L, v2eL = 6_000L, e2v = 7_000L, v2e = 8_000L;
        Mockito.when(stats.connections(vertexLabelFilter, LabelFilter.empty(Edge.class))).thenReturn(vL2e);
        Mockito.when(stats.connections(edgeLabelFilter, LabelFilter.empty(Vertex.class))).thenReturn(eL2v);
        Mockito.when(stats.connections(LabelFilter.empty(Edge.class), vertexLabelFilter)).thenReturn(e2vL);
        Mockito.when(stats.connections(LabelFilter.empty(Vertex.class), edgeLabelFilter)).thenReturn(v2eL);
        Mockito.when(stats.connections(LabelFilter.empty(Edge.class), LabelFilter.empty(Vertex.class))).thenReturn(e2v);
        Mockito.when(stats.connections(LabelFilter.empty(Vertex.class), LabelFilter.empty(Edge.class))).thenReturn(v2e);

        long incoming = 1_000_000L;

        PatternVertex vertex = MockedElementUtils.mockVertex(incoming, null);
        PatternVertex labeledVertex = MockedElementUtils.mockVertex(incoming, vertexLabelFilter);
        PatternEdge edge = MockedElementUtils.mockEdge(incoming, null);
        PatternEdge labeledEdge = MockedElementUtils.mockEdge(incoming, edgeLabelFilter);

        PatternVertex vertexWithProperties = MockedElementUtils.mockVertex(incoming, null,
                lowSelectivityVertexProperty, highSelectivityVertexProperty);
        PatternVertex labeledVertexWithProperties = MockedElementUtils.mockVertex(incoming, vertexLabelFilter,
                lowSelectivityVertexProperty, highSelectivityVertexProperty);
        PatternEdge edgeWithProperties = MockedElementUtils.mockEdge(incoming, null,
                lowSelectivityEdgeProperty, highSelectivityEdgeProperty);
        PatternEdge labeledEdgeWithProperties = MockedElementUtils.mockEdge(incoming, edgeLabelFilter,
                lowSelectivityEdgeProperty, highSelectivityEdgeProperty);

        return Stream.of(
                // element: no label, no properties; source: no label
                Arguments.of(vertex, edge, stats, Direction.IN, incoming * e2v / eTotal),
                Arguments.of(edge, vertex, stats, Direction.IN, incoming * v2e / vTotal),
                Arguments.of(vertex, edge, stats, Direction.OUT, incoming * v2e / eTotal),
                Arguments.of(edge, vertex, stats, Direction.OUT, incoming * e2v / vTotal),

                // element: label, no properties; source: no label
                Arguments.of(labeledVertex, edge, stats, Direction.IN, incoming * e2vL / eTotal),
                Arguments.of(labeledEdge, vertex, stats, Direction.IN, incoming * v2eL / vTotal),
                Arguments.of(labeledVertex, edge, stats, Direction.OUT, incoming * vL2e / eTotal),
                Arguments.of(labeledEdge, vertex, stats, Direction.OUT, incoming * eL2v / vTotal),

                // element: no label, no properties; source: label
                Arguments.of(vertex, labeledEdge, stats, Direction.IN, incoming * eL2v / eLabel),
                Arguments.of(edge, labeledVertex, stats, Direction.IN, incoming * vL2e / vLabel),
                Arguments.of(vertex, labeledEdge, stats, Direction.OUT, incoming * v2eL / eLabel),
                Arguments.of(edge, labeledVertex, stats, Direction.OUT, incoming * e2vL / vLabel),

                // element: label, no properties; source: label
                Arguments.of(labeledVertex, labeledEdge, stats, Direction.IN, incoming * eL2vL / eLabel),
                Arguments.of(labeledEdge, labeledVertex, stats, Direction.IN, incoming * vL2eL / vLabel),
                Arguments.of(labeledVertex, labeledEdge, stats, Direction.OUT, incoming * vL2eL / eLabel),
                Arguments.of(labeledEdge, labeledVertex, stats, Direction.OUT, incoming * eL2vL / vLabel),

                // element: no label, properties; source: no label
                Arguments.of(vertexWithProperties, edge, stats, Direction.IN, incoming * e2v / eTotal),
                Arguments.of(edgeWithProperties, vertex, stats, Direction.IN, incoming * v2e / vTotal),
                Arguments.of(vertexWithProperties, edge, stats, Direction.OUT, incoming * v2e / eTotal),
                Arguments.of(edgeWithProperties, vertex, stats, Direction.OUT, incoming * e2v / vTotal),

                // element: label, properties; source: no label
                Arguments.of(labeledVertexWithProperties, edge, stats, Direction.IN, incoming * e2vL / eTotal * vHighSel / vLabel),
                Arguments.of(labeledEdgeWithProperties, vertex, stats, Direction.IN, incoming * v2eL / vTotal * eHighSel / eLabel),
                Arguments.of(labeledVertexWithProperties, edge, stats, Direction.OUT, incoming * vL2e / eTotal * vHighSel / vLabel),
                Arguments.of(labeledEdgeWithProperties, vertex, stats, Direction.OUT, incoming * eL2v / vTotal * eHighSel / eLabel),

                // element: no label, properties; source: label
                Arguments.of(vertexWithProperties, labeledEdge, stats, Direction.IN, incoming * eL2v / eLabel),
                Arguments.of(edgeWithProperties, labeledVertex, stats, Direction.IN, incoming * vL2e / vLabel),
                Arguments.of(vertexWithProperties, labeledEdge, stats, Direction.OUT, incoming * v2eL / eLabel),
                Arguments.of(edgeWithProperties, labeledVertex, stats, Direction.OUT, incoming * e2vL / vLabel),

                // element: label, properties; source: label
                Arguments.of(labeledVertexWithProperties, labeledEdge, stats, Direction.IN, incoming * eL2vL / eLabel * vHighSel / vLabel),
                Arguments.of(labeledEdgeWithProperties, labeledVertex, stats, Direction.IN, incoming * vL2eL / vLabel * eHighSel / eLabel),
                Arguments.of(labeledVertexWithProperties, labeledEdge, stats, Direction.OUT, incoming * vL2eL / eLabel * vHighSel / vLabel),
                Arguments.of(labeledEdgeWithProperties, labeledVertex, stats, Direction.OUT, incoming * eL2vL / vLabel * eHighSel / eLabel)
        );
    }

    @Test
    public void testConstructor() {
        DependentRetrieval<?> r = Mockito.mock(DependentRetrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        assertEquals(Retrieval.IMPOSSIBLE, r.getEstimatedSize());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testDirectSelfDependency() {
        DependentRetrieval<Vertex> r = Mockito.mock(DependentRetrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        PatternElement<Vertex> v = MockedElementUtils.mockVertex(null);
        Mockito.when(r.getElement()).thenReturn(v);
        Mockito.when(r.getSource()).thenReturn((PatternElement) v);
        Mockito.when(v.getBestRetrieval()).thenReturn(r);

        assertTrue(r.isSelfDependent());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testIndirectSelfDependency() {
        DependentRetrieval<Vertex> r = Mockito.mock(DependentRetrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        DependentRetrieval<Vertex> r2 = Mockito.mock(DependentRetrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        PatternElement<Vertex> v = MockedElementUtils.mockVertex(null);
        PatternElement<Vertex> v2 = MockedElementUtils.mockVertex(null);
        Mockito.when(r.getElement()).thenReturn(v2);
        Mockito.when(r.getSource()).thenReturn((PatternElement) v);
        Mockito.when(v.getBestRetrieval()).thenReturn(r2);
        Mockito.when(r2.getElement()).thenReturn(v);
        Mockito.when(r2.getSource()).thenReturn((PatternElement) v2);
        Mockito.when(v2.getBestRetrieval()).thenReturn(r);

        assertTrue(r.isSelfDependent());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testNoSelfDependency() {
        DependentRetrieval<Vertex> r = Mockito.mock(DependentRetrieval.class,
                Mockito.withSettings().useConstructor().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        DirectRetrieval<Vertex> r2 = Mockito.mock(DirectRetrieval.class);
        PatternElement<Vertex> v = MockedElementUtils.mockVertex(null);
        PatternElement<Vertex> v2 = MockedElementUtils.mockVertex(null);
        Mockito.when(r.getElement()).thenReturn(v2);
        Mockito.when(r.getSource()).thenReturn((PatternElement) v);
        Mockito.when(v.getBestRetrieval()).thenReturn(r2);

        assertFalse(r.isSelfDependent());
    }

    @ParameterizedTest
    @MethodSource("generateTestElements")
    @SuppressWarnings("rawtypes")
    public void testEstimation(PatternElement element, PatternElement source, StatisticsProvider stats,
                               Direction direction, long expectedEstimation) {
        DependentRetrieval r = Mockito.mock(DependentRetrieval.class,
                Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(r.getElement()).thenReturn(element);
        Mockito.when(r.getSource()).thenReturn(source);
        Mockito.when(r.getDirection()).thenReturn(direction);

        r.estimate(stats);
        long estimation = r.getEstimatedSize();

        assertEquals(expectedEstimation, estimation);
    }
}