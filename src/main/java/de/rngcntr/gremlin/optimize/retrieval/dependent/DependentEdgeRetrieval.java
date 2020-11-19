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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Map;

/**
 * @author Florian Grieskamp
 *
 * Extends {@link DependentRetrieval} by the specification of a edge to be retrieved.
 */
public class DependentEdgeRetrieval extends DependentRetrieval<Edge> {
    /**
     * Creates a dependent edge retrieval and estimates it as impossible.
     *
     * @param edge The edge to be retrieved.
     * @param source The source vertex.
     * @param direction The direction of the retrieval. <ul>
     *                  <li><code>IN</code> if <code>edge</code> is an outgoing edge of <code>vertex</code></li>
     *                  <li><code>OUT</code> if <code>edge</code> is an outgoing edge of <code>vertex</code></li>
     * </ul>
     */
    public DependentEdgeRetrieval(PatternEdge edge, PatternVertex source, Direction direction) {
        super();
        this.element = edge;
        this.source = source;
        this.direction = direction;
    }

    /**
     * Creates a local Gremlin traversal that starts at the source vertex and contains the a following step that
     * retrieves the edge. The traversal is meant to be encapsulated within a Gremlin match step and
     * does not yet filter on label or property constraints.
     *
     * @return The local Gremlin traversal.
     */
    @Override
    protected GraphTraversal<?, Edge> getBaseTraversal() {
        GraphTraversal.Admin<?, Edge> t = new DefaultGraphTraversal<>();
        t = t.as(String.valueOf(source.getId())).asAdmin();
        t.addStep(new VertexStep<>(t, Edge.class, direction.opposite()));
        return t;
    }
}