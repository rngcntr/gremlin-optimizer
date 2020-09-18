package de.rngcntr.gremlin.optimize.retrieval.direct;

import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class DirectVertexRetrieval extends DirectRetrieval<Vertex> {
    public DirectVertexRetrieval(Class<Vertex> retrievedType, PatternVertex vertex) {
        super(retrievedType);
        this.element = vertex;
    }

    @Override
    protected GraphTraversal<Vertex, Vertex> getBaseTraversal() {
        GraphTraversal.Admin<Vertex,Vertex> t = new DefaultGraphTraversal<>();
        t.addStep(new GraphStep<Vertex,Vertex>(t, Vertex.class, true));
        return t;
    }
}
