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

import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirectVertexRetrievalTests {
    @Test
    public void testConstructor() {
        PatternVertex v = Mockito.mock(PatternVertex.class);
        DirectVertexRetrieval r = Mockito.mock(DirectVertexRetrieval.class,
                Mockito.withSettings().useConstructor(v).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        assertEquals(Retrieval.IMPOSSIBLE, r.getEstimatedSize());
        assertEquals(v, r.getElement());
    }

    @Test
    public void testBaseTraversal() {
        PatternVertex v = Mockito.mock(PatternVertex.class);
        DirectVertexRetrieval r = Mockito.mock(DirectVertexRetrieval.class,
                Mockito.withSettings().useConstructor(v).defaultAnswer(Mockito.CALLS_REAL_METHODS));

        final GraphTraversal<Vertex, Vertex> baseTraversal = r.getBaseTraversal();

        assertEquals(1, baseTraversal.asAdmin().getSteps().size());
        assertTrue(baseTraversal.asAdmin().getStartStep() instanceof GraphStep);
        GraphStep<?,?> startStep = (GraphStep<?, ?>) baseTraversal.asAdmin().getStartStep();
        assertTrue(startStep.returnsVertex());
        assertTrue(startStep.isStartStep());
        assertEquals(baseTraversal, startStep.getTraversal());
    }
}