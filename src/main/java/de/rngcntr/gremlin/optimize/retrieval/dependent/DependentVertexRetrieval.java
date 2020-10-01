package de.rngcntr.gremlin.optimize.retrieval.dependent;

import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class DependentVertexRetrieval extends DependentRetrieval<Vertex> {
    public DependentVertexRetrieval(Class<Vertex> retrievedType, PatternVertex vertex, PatternEdge source, Direction direction) {
        super(retrievedType);
        this.element = vertex;
        this.source = source;
        this.direction = direction;
    }

    @Override
    protected GraphTraversal<?, Vertex> getBaseTraversal() {
        GraphTraversal.Admin<?, Vertex> t = new DefaultGraphTraversal<>();
        t = t.as(String.valueOf(source.getId())).asAdmin();
        t.addStep(new EdgeVertexStep(t, direction));
        return t;
    }
}
