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

package de.rngcntr.gremlin.optimize.testutils.structure;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectEdgeRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectVertexRetrieval;
import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.mockito.Mockito;

import java.util.Arrays;

public class MockedElementUtils {
    public static PatternVertex mockVertex(long estimatedAmount, LabelFilter<Vertex> labelFilter, PropertyFilter<Vertex>... propertyFilters) {
        PatternVertex mockedVertex = mockVertex(labelFilter, propertyFilters);
        DirectVertexRetrieval mockedRetrieval = Mockito.mock(DirectVertexRetrieval.class);
        Mockito.when(mockedRetrieval.getEstimatedSize()).thenReturn(estimatedAmount);
        Mockito.when(mockedVertex.getBestRetrieval()).thenReturn(mockedRetrieval);
        return mockedVertex;
    }

    @SafeVarargs
    public static PatternVertex mockVertex(LabelFilter<Vertex> labelFilter, PropertyFilter<Vertex>... propertyFilters) {
        PatternVertex mockedVertex = Mockito.mock(PatternVertex.class);
        Mockito.when(mockedVertex.getType()).thenReturn(Vertex.class);
        Mockito.when(mockedVertex.hasLabelFilter()).thenReturn(labelFilter != null);
        Mockito.when(mockedVertex.getLabelFilter()).thenReturn(labelFilter);
        Mockito.when(mockedVertex.getPropertyFilters()).thenReturn(Arrays.asList(propertyFilters));
        return mockedVertex;
    }

    public static PatternEdge mockEdge(long estimatedAmount, LabelFilter<Edge> labelFilter, PropertyFilter<Edge>... propertyFilters) {
        PatternEdge mockedEdge = mockEdge(labelFilter, propertyFilters);
        DirectEdgeRetrieval mockedRetrieval = Mockito.mock(DirectEdgeRetrieval.class);
        Mockito.when(mockedRetrieval.getEstimatedSize()).thenReturn(estimatedAmount);
        Mockito.when(mockedEdge.getBestRetrieval()).thenReturn(mockedRetrieval);
        return mockedEdge;
    }

    @SafeVarargs
    public static PatternEdge mockEdge(LabelFilter<Edge> labelFilter, PropertyFilter<Edge>... propertyFilters) {
        PatternEdge mockedEdge = Mockito.mock(PatternEdge.class);
        Mockito.when(mockedEdge.getType()).thenReturn(Edge.class);
        Mockito.when(mockedEdge.hasLabelFilter()).thenReturn(labelFilter != null);
        Mockito.when(mockedEdge.getLabelFilter()).thenReturn(labelFilter);
        Mockito.when(mockedEdge.getPropertyFilters()).thenReturn(Arrays.asList(propertyFilters));
        return mockedEdge;
    }
}
