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

public class DirectEdgeRetrieval extends DirectRetrieval<Edge> {
    public DirectEdgeRetrieval(Class<Edge> retrievedType, PatternEdge edge) {
        super(retrievedType);
        this.element = edge;
    }

    @Override
    protected GraphTraversal<Edge, Edge> getBaseTraversal() {
        GraphTraversal.Admin<Edge,Edge> t = new DefaultGraphTraversal<>();
        t.addStep(new GraphStep<Edge,Edge>(t, Edge.class, true));
        return t;
    }
}