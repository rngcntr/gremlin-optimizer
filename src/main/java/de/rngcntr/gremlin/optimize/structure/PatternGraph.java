package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.util.GremlinWriter;
import de.rngcntr.gremlin.optimize.util.GremlinParser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.*;
import java.util.stream.Collectors;

public class PatternGraph {
    private List<PatternElement<?>> elements;
    private Map<PatternElement<?>, String> elementsToReturn;
    private final Graph sourceGraph;

    public PatternGraph(GraphTraversal<?,?> t) {
        elements = new ArrayList<>();
        elementsToReturn = new HashMap<>();
        sourceGraph = t.asAdmin().getGraph().orElse(null);
        buildGraphFromTraversal(t);
    }

    private void buildGraphFromTraversal(GraphTraversal<?,?> t) {
        GremlinParser parser = new GremlinParser();
        parser.parse(t);

        elements = parser.getElements();
        elementsToReturn = parser.getElementsToReturn();
    }

    public GraphTraversal<?,?> optimize(StatisticsProvider stats) {
        // 1st step: initialization of the graph and estimation of direct retrievals
        elements.forEach(PatternElement::initializeRetrievals);
        elements.forEach(e -> e.estimateDirectRetrievals(stats));

        // 2nd step: mark most selective PatternElement as final
        ArrayList<PatternElement<?>> leftElements = new ArrayList<>(elements);
        PatternElement<?> mostSelective = Collections.min(leftElements);
        leftElements.remove(mostSelective);
        mostSelective.makeFinal();

        // n-th step
        while (!leftElements.isEmpty()) {
            Collection<PatternElement<?>> neighbors = mostSelective.getNeighbors();
            neighbors.removeIf(PatternElement::isFinal);
            neighbors.forEach(e -> e.estimateDependentRetrievals(stats));
            mostSelective = Collections.min(neighbors);
            leftElements.remove(mostSelective);
            mostSelective.makeFinal();
        }

        return buildTraversal();
    }

    private GraphTraversal<?,?> buildTraversal() {
        return GremlinWriter.buildTraversal(this);
    }

    public List<PatternElement<?>> getElements() {
        return elements;
    }

    public Map<PatternElement<?>, String> getElementsToReturn() {
        return elementsToReturn;
    }

    public List<PatternVertex> getVertices() {
        return elements.stream()
                .filter(e -> e instanceof PatternVertex)
                .map(e -> (PatternVertex) e)
                .collect(Collectors.toList());
    }

    public List<PatternEdge> getEdges() {
        return elements.stream()
                .filter(e -> e instanceof PatternEdge)
                .map(e -> (PatternEdge) e)
                .collect(Collectors.toList());
    }

    public Graph getSourceGraph() {
        return sourceGraph;
    }

    @Override
    public String toString() {
        return elements.stream().map(PatternElement::toString)
                .collect(Collectors.joining("\n", "Graph consisting of:\n", ""));
    }
}
