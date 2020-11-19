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

import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.Map;

/**
 * @author Florian Grieskamp
 *
 * Extends {@link DirectRetrieval} by the specification of a edge to be retrieved.
 */
public class DirectEdgeRetrieval extends DirectRetrieval<Edge> {
    /**
     * Creates a direct edge retrieval and estimates it as impossible.
     *
     * @param edge The edge to be retrieved.
     */
    public DirectEdgeRetrieval(PatternEdge edge) {
        super();
        this.element = edge;
    }

    /**
     * Creates a global Gremlin traversal that retrieves all edge candidates but does not yet filter on label or
     * property constraints.
     *
     * @return The global Gremlin traversal.
     */
    @Override
    protected GraphTraversal<Edge, Edge> getBaseTraversal() {
        GraphTraversal.Admin<Edge, Edge> t = new DefaultGraphTraversal<>();
        t.addStep(new GraphStep<Edge, Edge>(t, Edge.class, true));
        return t;
    }
}