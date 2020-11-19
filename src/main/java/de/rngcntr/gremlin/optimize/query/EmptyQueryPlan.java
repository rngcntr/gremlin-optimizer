package de.rngcntr.gremlin.optimize.query;

import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Collections;
import java.util.Set;

public class EmptyQueryPlan implements PartialQueryPlan {
    @Override
    public Set<PatternElement<?>> getElements() {
        return Collections.emptySet();
    }

    @Override
    public GraphTraversal<Object, Object> asTraversal() {
        return new DefaultGraphTraversal<>();
    }

    @Override
    public Set<PartialQueryPlan> generalCut(Set<PatternElement<?>> elementsToKeep) {
        return Collections.emptySet();
    }

    @Override
    public Set<DependencyTree> explicitCut(Set<PatternElement<?>> elementsToKeep) {
        return Collections.emptySet();
    }

    @Override
    public boolean isMovable() {
        return false;
    }

    @Override
    public String toString() {
        return "empty";
    }
}
