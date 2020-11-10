package de.rngcntr.gremlin.optimize.query;

import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.step.JoinStep;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.util.GremlinWriter;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Join implements PartialQueryPlan {

    private PartialQueryPlan left;
    private PartialQueryPlan right;
    Set<PartialQueryPlan> after;
    Set<PatternElement<?>> joinAttributes;

    public Join(PartialQueryPlan left, PartialQueryPlan right) {
        this.left = left;
        this.right = right;
        joinAttributes = new HashSet<>();
        joinAttributes.addAll(left.getElements());
        joinAttributes.retainAll(right.getElements());
        after = new HashSet<>();
        rearrange();
    }

    private void rearrange() {
        if (joinAttributes.isEmpty()) {
            if (left.isMovable()) {
                after.add(left);
                left = new EmptyQueryPlan();
            }
            if (right.isMovable()) {
                after.add(right);
                right = new EmptyQueryPlan();
            }
        } else {
            after.addAll(left.cut(joinAttributes));
            after.addAll(right.cut(joinAttributes));
        }
    }

    @Override
    public Set<PatternElement<?>> getElements() {
        Set<PatternElement<?>> elements = new HashSet<>();
        elements.addAll(left.getElements());
        elements.addAll(right.getElements());
        after.forEach(pqp -> elements.addAll(pqp.getElements()));
        return elements;
    }

    @Override
    public GraphTraversal<Map<String, Object>, Map<String, Object>> asTraversal() {
        final GraphTraversal.Admin<Map<String, Object>, Map<String, Object>> leftAdmin = left.asTraversal().asAdmin();
        final GraphTraversal.Admin<Map<String, Object>, Map<String, Object>> rightAdmin = right.asTraversal().asAdmin();
        final JoinStep joinStep = new JoinStep(leftAdmin, rightAdmin,
                joinAttributes.stream().map(PatternElement::getId).map(String::valueOf).collect(Collectors.toSet()));
        GremlinWriter.selectElements(leftAdmin, left.getElements(), true);
        leftAdmin.addStep(joinStep);
        after.forEach(pqp -> {
            TraversalHelper.insertTraversal(joinStep, pqp.asTraversal().asAdmin(), leftAdmin);
            //leftAdmin.match(pqp.asTraversal());
        });
        //return GremlinWriter.selectElements(leftAdmin, getElements(), true);
        return leftAdmin;
    }

    @Override
    public Set<PartialQueryPlan> cut(Set<PatternElement<?>> elementsToKeep) {
        Set<PartialQueryPlan> cutParts = new HashSet<>();
        Set<PartialQueryPlan> removedChildren = new HashSet<>();

        after.forEach(pqp -> {
            final Set<PatternElement<?>> pqpElements = pqp.getElements();
            pqpElements.retainAll(elementsToKeep);
            if (pqpElements.isEmpty()) {
                cutParts.add(pqp);
                removedChildren.add(pqp);
            } else {
                cutParts.addAll(pqp.cut(elementsToKeep));
            }
        });
        after.removeAll(removedChildren);

        return cutParts;
    }

    @Override
    public boolean isMovable() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("Join on {%s} trees {%s, %s} then {%s}",
                joinAttributes.stream().map(PatternElement::getId).map(String::valueOf).collect(Collectors.joining(", ")),
                left, right,
                after.stream().map(PartialQueryPlan::toString).collect(Collectors.joining(", "))
        );
    }
}
