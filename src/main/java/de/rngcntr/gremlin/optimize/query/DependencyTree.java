package de.rngcntr.gremlin.optimize.query;

import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import de.rngcntr.gremlin.optimize.util.GremlinWriter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DependencyTree implements PartialQueryPlan {

    private final Retrieval<?> root;
    private final Set<DependencyTree> children;

    public DependencyTree(Retrieval<?> root) {
        this.root = root;
        children = new HashSet<>();
    }

    public static DependencyTree of(Retrieval<?> root) {
        DependencyTree tree = new DependencyTree(root);
        Collection<PatternElement<?>> dependentNeighbors = root.getElement().getDependentNeighbors();
        dependentNeighbors.forEach(pe -> tree.children.add(DependencyTree.of(pe.getBestRetrieval())));
        ensureVertexClosure(tree, dependentNeighbors);
        return tree;
    }

    private static void ensureVertexClosure(DependencyTree tree, Collection<PatternElement<?>> alreadyCoveredElements) {
        PatternElement<?> rootElement = tree.root.getElement();
        if (rootElement instanceof PatternEdge) {
            // pattern edges may not have open ends
            if (rootElement.getBestRetrieval() instanceof DependentRetrieval) {
                alreadyCoveredElements.add(((DependentRetrieval<?>) rootElement.getBestRetrieval()).getSource());
            }
            rootElement.getNeighbors(Direction.BOTH).stream()
                    .filter(pe -> alreadyCoveredElements.stream().noneMatch(coveredElem -> coveredElem.getId() == pe.getId()))
                    .forEach(pe -> {
                            assert pe.getDependentRetrieval(rootElement).isPresent();
                            tree.children.add(new DependencyTree(pe.getDependentRetrieval(rootElement).get()));
                    });
        }
    }

    public <E> Set<E> getRecursive(Function<Retrieval<?>, E> mapper) {
        final Set<E> set = new HashSet<>(Collections.singleton(mapper.apply(root)));
        children.forEach(c -> set.addAll(c.getRecursive(mapper)));
        return set;
    }

    public GraphTraversal<Object, Object> asTraversal() {
        final Set<Retrieval<?>> allRetrievals = getRecursive(ret -> ret);
        GraphTraversal<?,?> assembledTraversal;
        if (root instanceof DirectRetrieval) {
            assembledTraversal = root.asTraversal();
            allRetrievals.remove(root);
        } else {
            assembledTraversal = new DefaultGraphTraversal<>();
            final DependentRetrieval<?> dependentRoot = (DependentRetrieval<?>) root;
            assembledTraversal.select(String.valueOf(dependentRoot.getSource().getId()));
        }
        if (allRetrievals.size() > 0) {
            assembledTraversal = assembledTraversal.match(allRetrievals.stream()
                    .map(Retrieval::asTraversal).toArray(GraphTraversal[]::new));
        }

        //return (GraphTraversal<Object, Object>) GremlinWriter.selectElements(assembledTraversal, getElements(), true);
        return (GraphTraversal<Object, Object>) assembledTraversal;
    }

    @Override
    public Set<PartialQueryPlan> generalCut(Set<PatternElement<?>> elementsToKeep) {
        Set<PartialQueryPlan> cutBranches = new HashSet<>();
        Set<DependencyTree> removedChildren = new HashSet<>();

        children.forEach(c -> {
            Set<PatternElement<?>> childrenElements = c.getElements();
            childrenElements.retainAll(elementsToKeep);
            if (childrenElements.isEmpty()) {
                cutBranches.add(c);
                removedChildren.add(c);
            } else {
                cutBranches.addAll(c.generalCut(elementsToKeep));
            }
        });
        children.removeAll(removedChildren);

        return cutBranches;
    }

    @Override
    public Set<DependencyTree> explicitCut(Set<PatternElement<?>> borderElements) {
        Set<DependencyTree> cutBranches = new HashSet<>();
        Set<DependencyTree> removedChildren = new HashSet<>();

        children.forEach(c -> {
            if (!c.children.isEmpty()) {
                cutBranches.addAll(c.explicitCut(borderElements));
            } else {
                if (borderElements.contains(c.root.getElement())
                        && c.getRoot().getElement() instanceof PatternVertex
                        && c.getRoot() instanceof DependentRetrieval) {
                    cutBranches.add(c);
                    removedChildren.add(c);
                }
            }
        });
        children.removeAll(removedChildren);

        return cutBranches;
    }

    @Override
    public boolean isMovable() {
        // Gremlin does not support E() steps in the middle of a query, so these must remain stationary
        return !(root instanceof DirectRetrieval);
    }

    @Override
    public Set<PatternElement<?>> getElements() {
        return getRecursive(Retrieval::getElement);
    }

    public Retrieval<?> getRoot() {
        return root;
    }

    @Override
    public String toString() {
        return String.format("%s -> {%s}", root, children.stream().map(DependencyTree::toString).collect(Collectors.joining(", ")));
    }
}
