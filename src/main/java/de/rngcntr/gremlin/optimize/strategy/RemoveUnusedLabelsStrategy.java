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

package de.rngcntr.gremlin.optimize.strategy;

import de.rngcntr.gremlin.optimize.query.JoinAttribute;
import de.rngcntr.gremlin.optimize.step.JoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.*;

public class RemoveUnusedLabelsStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final RemoveUnusedLabelsStrategy INSTANCE = new RemoveUnusedLabelsStrategy();

    public static RemoveUnusedLabelsStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return new HashSet<>(Collections.singletonList(RemoveRedundantSelectStrategy.class));
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        Set<String> selectedLabels = getSelectedLabels(traversal);
        TraversalHelper.getStepsOfAssignableClassRecursively(Step.class, traversal).forEach(s -> {
            Set<String> stepLabels = new HashSet<String>(s.getLabels());
            for (String stepLabel : stepLabels) {
                if (selectedLabels.stream().noneMatch(stepLabel::equals)) {
                    s.removeLabel(stepLabel);
                }
            }
        });
    }

    private static Set<String> getSelectedLabels(Traversal.Admin<?,?> traversal) {
        Set<String> selectedLabels = new HashSet<>();
        TraversalHelper.getStepsOfAssignableClassRecursively(Scoping.class, traversal).forEach(
                s -> selectedLabels.addAll(s.getScopeKeys())
        );
        TraversalHelper.getStepsOfAssignableClassRecursively(JoinStep.class, traversal).forEach(
                s -> s.getJoinAttributes().forEach(attr ->
                        ((JoinAttribute) attr).getElements().forEach(e ->
                                selectedLabels.add(String.valueOf(e.getId())))));
        return selectedLabels;
    }
}
