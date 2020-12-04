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

import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Florian Grieskamp
 *
 * Extends {@link DependentRetrieval} by the specification of a vertex to be retrieved.
 */
public class DependentVertexRetrieval extends DependentRetrieval<Vertex> {
    /**
     * Creates a dependent vertex retrieval and estimates it as impossible.
     *
     * @param vertex The vertex to be retrieved.
     * @param source The source edge.
     * @param direction The direction of the retrieval. <ul>
     *                  <li><code>IN</code> if <code>vertex</code> is the end vertex of <code>source</code></li>
     *                  <li><code>OUT</code> if <code>vertex</code> is the start vertex of <code>source</code></li>
     * </ul>
     */
    public DependentVertexRetrieval(PatternVertex vertex, PatternEdge source, Direction direction) {
        super();
        this.element = vertex;
        this.source = source;
        this.direction = direction;
    }

    /**
     * Creates a local Gremlin traversal that starts at the source edge and contains the a following step that
     * retrieves the vertex. The traversal is meant to be encapsulated within a Gremlin match step and
     * does not yet filter on label or property constraints.
     *
     * @return The local Gremlin traversal.
     */
    @Override
    protected GraphTraversal<?, Vertex> getBaseTraversal() {
        GraphTraversal.Admin<?, Vertex> t = new DefaultGraphTraversal<>();
        t = t.as(String.valueOf(source.getId())).asAdmin();
        t.addStep(new EdgeVertexStep(t, direction));
        return t;
    }
}