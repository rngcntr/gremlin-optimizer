// Copyright 2020 Florian Grieskamp
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package de.rngcntr.gremlin.optimize.query;

import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.step.JoinStep;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Join implements PartialQueryPlan {

    private PartialQueryPlan left;
    private PartialQueryPlan right;
    Set<PartialQueryPlan> directAfter;
    Set<PartialQueryPlan> generalAfter;
    Set<JoinAttribute> joinAttributes;

    public Join(PartialQueryPlan left, PartialQueryPlan right) {
        this.left = left;
        this.right = right;
        joinAttributes = new HashSet<>();
        joinAttributes.addAll(left.getElements().stream().map(JoinAttribute::new).collect(Collectors.toList()));
        joinAttributes.retainAll(right.getElements().stream().map(JoinAttribute::new).collect(Collectors.toList()));
        directAfter = new HashSet<>();
        generalAfter = new HashSet<>();
        generalRearrange();
        explicitRearrange();
    }

    private void generalRearrange() {
        if (joinAttributes.isEmpty()) {
            if (left.isMovable()) {
                generalAfter.add(left);
                left = new EmptyQueryPlan();
            }
            if (right.isMovable()) {
                generalAfter.add(right);
                right = new EmptyQueryPlan();
            }
        } else {
            generalAfter.addAll(left.generalCut(joinAttributes.stream().flatMap(a -> a.getElements().stream()).collect(Collectors.toSet())));
            generalAfter.addAll(right.generalCut(joinAttributes.stream().flatMap(a -> a.getElements().stream()).collect(Collectors.toSet())));
        }
    }

    /*
        TODO: rename
        rearranges the join such that vertices do not need to be fetched before joining
     */
    private void explicitRearrange() {
        final Set<DependencyTree> leftCut = left.explicitCut(joinAttributes.stream().flatMap(a -> a.getElements().stream()).collect(Collectors.toSet()));
        final Set<DependencyTree> rightCut = right.explicitCut(joinAttributes.stream().flatMap(a -> a.getElements().stream()).collect(Collectors.toSet()));
        for (DependencyTree dependencyTree : leftCut) {
            if (dependencyTree.getRoot() instanceof DependentRetrieval) {
                final DependentRetrieval<?> root = (DependentRetrieval<?>) dependencyTree.getRoot();
                joinAttributes.forEach(ja -> ja.reformat(root, JoinAttribute.JoinPosition.LEFT));
            }
        }
        for (DependencyTree dependencyTree : rightCut) {
            if (dependencyTree.getRoot() instanceof DependentRetrieval) {
                final DependentRetrieval<?> root = (DependentRetrieval<?>) dependencyTree.getRoot();
                joinAttributes.forEach(ja -> ja.reformat(root, JoinAttribute.JoinPosition.RIGHT));
            }
        }
        directAfter.addAll(leftCut);
        directAfter.addAll(rightCut);
    }

    @Override
    public Set<PatternElement<?>> getElements() {
        Set<PatternElement<?>> elements = new HashSet<>();
        elements.addAll(left.getElements());
        elements.addAll(right.getElements());
        directAfter.forEach(pqp -> elements.addAll(pqp.getElements()));
        generalAfter.forEach(pqp -> elements.addAll(pqp.getElements()));
        return elements;
    }

    @Override
    public GraphTraversal<Object, Object> asTraversal() {
        final GraphTraversal.Admin<Object, Object> leftAdmin = left.asTraversal().asAdmin();
        final GraphTraversal.Admin<Object, Object> rightAdmin = right.asTraversal().asAdmin();
        final JoinStep<?> joinStep = new JoinStep<>(leftAdmin, rightAdmin, joinAttributes);
        leftAdmin.addStep(joinStep);

        directAfter.forEach(pqp -> TraversalHelper.insertTraversal(leftAdmin.getEndStep(), pqp.asTraversal().asAdmin(), leftAdmin));
        generalAfter.forEach(pqp -> TraversalHelper.insertTraversal(leftAdmin.getEndStep(), pqp.asTraversal().asAdmin(), leftAdmin));
        return leftAdmin;
    }

    @Override
    public Set<PartialQueryPlan> generalCut(Set<PatternElement<?>> elementsToKeep) {
        Set<PartialQueryPlan> cutParts = new HashSet<>();
        Set<PartialQueryPlan> removedChildren = new HashSet<>();

        generalAfter.forEach(pqp -> {
            final Set<PatternElement<?>> pqpElements = pqp.getElements();
            pqpElements.retainAll(elementsToKeep);
            if (pqpElements.isEmpty()) {
                cutParts.add(pqp);
                removedChildren.add(pqp);
            } else {
                cutParts.addAll(pqp.generalCut(elementsToKeep));
            }
        });
        generalAfter.removeAll(removedChildren);

        return cutParts;
    }

    @Override
    public Set<DependencyTree> explicitCut(Set<PatternElement<?>> elementsToKeep) {
        // TODO implement
        return new HashSet<>();
    }

    @Override
    public boolean isMovable() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("Join on {%s} trees {%s, %s} then {%s}",
                joinAttributes.stream().map(String::valueOf).collect(Collectors.joining(", ")),
                left, right,
                Stream.concat(directAfter.stream(), generalAfter.stream()).map(PartialQueryPlan::toString).collect(Collectors.joining(", "))
        );
    }
}
