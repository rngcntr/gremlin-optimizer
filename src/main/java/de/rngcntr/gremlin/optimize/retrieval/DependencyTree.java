package de.rngcntr.gremlin.optimize.retrieval;

import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.util.GremlinWriter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.*;
import java.util.function.Function;

public class DependencyTree {

    private final Retrieval<?> root;
    private final Set<DependencyTree> children;

    public DependencyTree(Retrieval<?> root, boolean autoExtend) {
        this.root = root;
        children = new HashSet<>();
        if (autoExtend) {
            Collection<PatternElement<?>> dependentNeighbors = root.getElement().getDependentNeighbors();
            dependentNeighbors.forEach(pe -> children.add(new DependencyTree(pe.getBestRetrieval(), true)));
            ensureVertexClosure(dependentNeighbors);
        }
    }

    private void ensureVertexClosure(Collection<PatternElement<?>> alreadyCoveredElements) {
        PatternElement<?> rootElement = root.getElement();
        if (rootElement instanceof PatternEdge) {
            // pattern edges may not have open ends
            if (rootElement.getBestRetrieval() instanceof DependentRetrieval) {
                alreadyCoveredElements.add(((DependentRetrieval<?>) rootElement.getBestRetrieval()).getSource());
            }
            rootElement.getNeighbors(Direction.BOTH).stream()
                    .filter(pe -> !alreadyCoveredElements.contains(pe))
                    .forEach(pe -> {
                            assert pe.getDependentRetrieval(rootElement).isPresent();
                            children.add(new DependencyTree(pe.getDependentRetrieval(rootElement).get(), false));
                    });
        }
    }

    public <E> Set<E> getRecursive(Function<Retrieval<?>, E> mapper) {
        final Set<E> set = new HashSet<>(Collections.singleton(mapper.apply(root)));
        children.forEach(c -> set.addAll(c.getRecursive(mapper)));
        return set;
    }

    public GraphTraversal<Map<String, Object>, Map<String, Object>> asMatchTraversal() {
        GraphTraversal<Map<String,Object>,?> assembledTraversal = root.asTraversal();
        final Set<Retrieval<?>> allRetrievals = getRecursive(ret -> ret);
        allRetrievals.remove(root);
        if (allRetrievals.size() > 0) {
            assembledTraversal = assembledTraversal.match(allRetrievals.stream()
                    .map(Retrieval::asTraversal).toArray(GraphTraversal[]::new));
        }
        return GremlinWriter.selectElements(assembledTraversal, getRecursive(Retrieval::getElement), true);
    }
}
