package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentEdgeRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectEdgeRetrieval;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PatternEdge extends PatternElement<Edge> {
    private PatternVertex start;
    private PatternVertex end;

    public PatternEdge() {
        super(Edge.class);
    }

    public void setStart(PatternVertex start) {
        this.start = start;
    }

    public void setEnd(PatternVertex end) {
        this.end = end;
    }

    public void setVertex(PatternVertex neighbor, Direction direction) {
        if (direction == Direction.OUT) {
            setStart(neighbor);
        } else if (direction == Direction.IN) {
            setEnd(neighbor);
        }
    }

    @Override
    public DirectEdgeRetrieval generateDirectRetrieval() {
        return new DirectEdgeRetrieval(Edge.class, this);
    }

    @Override
    public Collection<DependentRetrieval<Edge>> generateDependentRetrievals() {
        ArrayList<DependentRetrieval<Edge>> retrievals = new ArrayList<>();
        retrievals.add(new DependentEdgeRetrieval(Edge.class, this, start, Direction.OUT));
        retrievals.add(new DependentEdgeRetrieval(Edge.class, this, end, Direction.IN));
        return retrievals;
    }

    @Override
    public List<PatternElement<?>> getNeighbors() {
        ArrayList<PatternElement<?>> neighbors = new ArrayList<>(2);
        neighbors.add(start);
        neighbors.add(end);
        return neighbors;
    }

    @Override
    public String toString() {
        String alias = stepLabel == null ? "" : String.format(" aka. \"%s\"", stepLabel);
        return String.format("Edge %d%s (%s)\n\tProperties: %s\n\tIn: %d\n\tOut: %d", id,
                alias, labelFilter, propertyFilters, start.getId(), end.getId());
    }
}
