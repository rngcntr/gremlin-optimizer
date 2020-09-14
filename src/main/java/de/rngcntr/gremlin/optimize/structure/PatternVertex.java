package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentVertexRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectVertexRetrieval;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class PatternVertex extends PatternElement<Vertex> {
    private final Collection<PatternEdge> in;
    private final Collection<PatternEdge> out;

    public PatternVertex() {
        super(Vertex.class);
        in = new ArrayList<>();
        out = new ArrayList<>();
    }

    public void addEdge(PatternEdge edge, Direction direction) {
        switch (direction) {
            case IN:
                in.add(edge);
                break;
            case OUT:
                out.add(edge);
                break;
            default:
                throw new IllegalArgumentException("Not a valid direction: " + direction);
        }
    }

    @Override
    public DirectRetrieval<Vertex> generateDirectRetrieval() {
        return new DirectVertexRetrieval(Vertex.class, this);
    }

    @Override
    public Collection<DependentRetrieval<Vertex>> generateDependentRetrievals() {
        ArrayList<DependentRetrieval<Vertex>> dependentRetrievals = new ArrayList<>();
        in.forEach(e -> dependentRetrievals.add(new DependentVertexRetrieval(Vertex.class, this, e, Direction.IN)));
        out.forEach(e -> dependentRetrievals.add(new DependentVertexRetrieval(Vertex.class, this, e, Direction.OUT)));
        return dependentRetrievals;
    }

    @Override
    public List<PatternElement<?>> getNeighbors() {
        ArrayList<PatternElement<?>> neighbors = new ArrayList<>(in.size() + out.size());
        neighbors.addAll(in);
        neighbors.addAll(out);
        return neighbors;
    }

    @Override
    public String toString() {
        String alias = stepLabel == null ? "" : String.format(" aka. \"%s\"", stepLabel);
        return String.format("Vertex %d%s (%s)\n\tProperties: %s\n\tIn: %s\n\tOut: %s", id,
                alias, labelFilter, propertyFilters,
                in.stream().map(PatternElement::getId).collect(Collectors.toList()),
                out.stream().map(PatternElement::getId).collect(Collectors.toList()));
    }
}