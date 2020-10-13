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
        if (start != null) {
            retrievals.add(new DependentEdgeRetrieval(Edge.class, this, start, Direction.IN));
        }
        if (end != null) {
            retrievals.add(new DependentEdgeRetrieval(Edge.class, this, end, Direction.OUT));
        }
        return retrievals;
    }

    @Override
    public List<PatternElement<?>> getNeighbors() {
        ArrayList<PatternElement<?>> neighbors = new ArrayList<>();
        if (start != null) {
            neighbors.add(start);
        }
        if (end != null) {
            neighbors.add(end);
        }
        return neighbors;
    }

    @Override
    public String toString() {
        String format = super.toString();
        String edgeSpecific = String.format("\n\tOut: %s\n\tIn: %s",
                start != null ? start.getId() : -1,
                end != null ? end.getId() : -1);
        return String.format(format, "EDGE", edgeSpecific);
    }
}
