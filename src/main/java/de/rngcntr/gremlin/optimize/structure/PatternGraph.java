package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.util.GremlinWriter;
import de.rngcntr.gremlin.optimize.util.GremlinParser;
import de.rngcntr.gremlin.optimize.util.Permutations;
import org.apache.commons.lang.StringUtils;
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

        Set<PatternElement<?>> updateRequired = new HashSet<>(elements);
        PatternElement<?> startingPoint = Collections.min(updateRequired);
        updateRequired.remove(startingPoint);

        // n-th step
        while (!updateRequired.isEmpty()) {
            PatternElement<?> elementToUpdate = updateRequired.iterator().next();
            updateRequired.remove(elementToUpdate);
            long sizeBeforeUpdate = elementToUpdate.getBestRetrieval().getEstimatedSize();
            elementToUpdate.estimateDependentRetrievals(stats);
            long sizeAfterUpdate = elementToUpdate.getBestRetrieval().getEstimatedSize();

            if (sizeAfterUpdate < sizeBeforeUpdate) {
                updateRequired.addAll(elementToUpdate.getNeighbors());
            }
        }

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
    public boolean equals(Object other) {
        if (!(other instanceof PatternGraph)) return false;
        PatternGraph otherGraph = (PatternGraph) other;
        if (!sourceGraph.equals(otherGraph.sourceGraph)) return false;
        if (elements.size() != otherGraph.elements.size()) return false;
        if (elementsToReturn.size() != otherGraph.elementsToReturn.size()) return false;

        Set<Map<PatternElement<?>, PatternElement<?>>> possibleElementMappings = new HashSet<>();

        Permutations.of(otherGraph.elements).forEach(p -> {
            List<PatternElement<?>> permutation = p.collect(Collectors.toList());
            Map<PatternElement<?>, PatternElement<?>> elementMapping = new HashMap<>();
            for (int i = 0; i < elements.size(); ++i) {
                if (!elements.get(i).equals(permutation.get(i))) return;
                elementMapping.put(elements.get(i), permutation.get(i));
            }
            for (int i = 0; i < elements.size(); ++i) {
                if (!elements.get(i).isIsomorphicTo(permutation.get(i), elementMapping)) return;
            }
            possibleElementMappings.add(elementMapping);
        });

        mappingLoop: for (Map<PatternElement<?>, PatternElement<?>> elementMapping : possibleElementMappings) {
            for (PatternElement<?> element : elementsToReturn.keySet()) {
                if (!otherGraph.elementsToReturn.containsKey(elementMapping.get(element))) {continue mappingLoop;}
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return elements.stream().map(PatternElement::toString)
                .collect(Collectors.joining("\n", "Graph consisting of:\n", ""));
    }
}
