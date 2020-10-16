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

import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DependentEdgeRetrievalTests {
    @ParameterizedTest
    @EnumSource(Direction.class)
    public void testConstructor(Direction d) {
        PatternEdge e = Mockito.mock(PatternEdge.class);
        PatternVertex v = Mockito.mock(PatternVertex.class);
        DependentEdgeRetrieval r = Mockito.mock(DependentEdgeRetrieval.class,
                Mockito.withSettings().useConstructor(e, v, d).defaultAnswer(Mockito.CALLS_REAL_METHODS));

        assertEquals(Retrieval.IMPOSSIBLE, r.getEstimatedSize());
        assertEquals(e, r.getElement());
        assertEquals(v, r.getSource());
        assertEquals(d, r.getDirection());
    }

    @ParameterizedTest
    @EnumSource(Direction.class)
    public void testBaseTraversal(Direction d) {
        PatternEdge e = Mockito.mock(PatternEdge.class);
        PatternVertex v = Mockito.mock(PatternVertex.class);
        long id = 12345L;
        Mockito.when(v.getId()).thenReturn(id);
        DependentEdgeRetrieval r = Mockito.mock(DependentEdgeRetrieval.class,
                Mockito.withSettings().useConstructor(e, v, d).defaultAnswer(Mockito.CALLS_REAL_METHODS));

        final GraphTraversal<?, Edge> baseTraversal = r.getBaseTraversal();

        assertEquals(2, baseTraversal.asAdmin().getSteps().size());
        assertTrue(baseTraversal.asAdmin().getStartStep() instanceof StartStep);
        StartStep<?> startStep = (StartStep<?>) baseTraversal.asAdmin().getStartStep();
        assertEquals(1, startStep.getLabels().size());
        assertEquals(String.valueOf(id), startStep.getLabels().iterator().next());
        assertTrue(startStep.getNextStep() instanceof VertexStep);
        VertexStep<?> vertexStep = (VertexStep<?>) startStep.getNextStep();
        assertTrue(vertexStep.returnsEdge());
        assertEquals(baseTraversal, startStep.getTraversal());
        assertEquals(baseTraversal, vertexStep.getTraversal());
    }
}
