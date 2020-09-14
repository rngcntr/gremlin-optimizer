package de.rngcntr.gremlin.optimize.retrieval.dependent;

import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class DependentEdgeRetrieval extends DependentRetrieval<Edge> {
    public DependentEdgeRetrieval(Class<Edge> retrievedType, PatternEdge edge, PatternVertex source, Direction direction) {
        super(retrievedType);
        this.element = edge;
        this.source = source;
        t.addStep(new VertexStep<>(t, Edge.class, direction));
    }
}
