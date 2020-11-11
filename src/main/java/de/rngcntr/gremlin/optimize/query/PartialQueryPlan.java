package de.rngcntr.gremlin.optimize.query;

import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Map;
import java.util.Set;

public interface PartialQueryPlan {
    Set<PatternElement<?>> getElements();
    GraphTraversal<Map<String, Object>, Map<String, Object>> asTraversal();
    Set<PartialQueryPlan> generalCut(Set<PatternElement<?>> elementsToKeep);
    Set<DependencyTree> explicitCut(Set<PatternElement<?>> elementsToKeep);
    boolean isMovable();
}
