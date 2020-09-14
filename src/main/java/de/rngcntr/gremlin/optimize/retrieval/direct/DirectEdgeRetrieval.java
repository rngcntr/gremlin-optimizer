package de.rngcntr.gremlin.optimize.retrieval.direct;

import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Edge;

public class DirectEdgeRetrieval extends DirectRetrieval<Edge> {
    public DirectEdgeRetrieval(Class<Edge> retrievedType, PatternEdge edge) {
        super(retrievedType);
        this.element = edge;
        t.addStep(new GraphStep<Edge,Edge>(t, Edge.class, true));
    }
}
