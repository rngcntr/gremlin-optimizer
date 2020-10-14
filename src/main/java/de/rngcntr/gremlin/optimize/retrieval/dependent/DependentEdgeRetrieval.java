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

public class DependentEdgeRetrieval extends DependentRetrieval<Edge> {
    public DependentEdgeRetrieval(Class<Edge> retrievedType, PatternEdge edge, PatternVertex source, Direction direction) {
        super(retrievedType);
        this.element = edge;
        this.source = source;
        this.direction = direction;
    }

    @Override
    protected GraphTraversal<?, Edge> getBaseTraversal() {
        GraphTraversal.Admin<?, Edge> t = new DefaultGraphTraversal<>();
        t = t.as(String.valueOf(source.getId())).asAdmin();
        t.addStep(new VertexStep<>(t, Edge.class, direction.opposite()));
        return t;
    }
}