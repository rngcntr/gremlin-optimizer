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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.*;

public class FlattenMatchStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final FlattenMatchStepStrategy INSTANCE = new FlattenMatchStepStrategy();

    public static FlattenMatchStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClassRecursively(MatchStep.class, traversal).forEach(FlattenMatchStepStrategy::tryToFlatten);
    }

    private static void tryToFlatten(MatchStep<Object,Object> matchStep) {
        String startLabel = getStartLabelOfLinearPattern(matchStep);
        if (startLabel != null) {
            flatten(matchStep, startLabel);
        }
    }

    private static String getStartLabelOfLinearPattern(MatchStep<Object, Object> matchStep) {
        Set<String> startLabels = new HashSet<>();
        Set<String> endLabels = new HashSet<>();

        for(Traversal.Admin<Object, Object> matchTraversal : matchStep.getGlobalChildren()) {
            assert MatchStep.Helper.getStartLabels(matchTraversal).size() == 1;
            assert MatchStep.Helper.getEndLabel(matchTraversal).isPresent();
            String startLabel = MatchStep.Helper.getStartLabels(matchTraversal).iterator().next();
            String endLabel = MatchStep.Helper.getEndLabel(matchTraversal).get();
            // each start and end label must only appear once in a linear pattern
            if (startLabels.contains(startLabel) || endLabels.contains(endLabel)) {
                return null;
            }
            startLabels.add(startLabel);
            endLabels.add(endLabel);
        }

        Set<String> openStartLabels = new HashSet<>(startLabels);
        Set<String> openEndLabels = new HashSet<>(endLabels);
        openStartLabels.removeAll(endLabels);
        openEndLabels.removeAll(startLabels);

        if (openStartLabels.size() == 1 && openEndLabels.size() == 1) {
            return openStartLabels.iterator().next();
        }

        return null;
    }

    private static void flatten(MatchStep<Object, Object> matchStep, String startLabel) {
        List<Traversal.Admin<Object, Object>> leftMatchSteps = new ArrayList<>(matchStep.getGlobalChildren());
        List<Traversal.Admin<Object, Object>> orderedTraversals = new ArrayList<>();
        String nextLabel = startLabel;

        while (!leftMatchSteps.isEmpty()) {
            for (Traversal.Admin<Object, Object> matchTraversal : leftMatchSteps) {
                if (MatchStep.Helper.getStartLabels(matchTraversal).contains(nextLabel)) {
                    leftMatchSteps.remove(matchTraversal);
                    orderedTraversals.add(matchTraversal);
                    assert MatchStep.Helper.getEndLabel(matchTraversal).isPresent();
                    nextLabel = MatchStep.Helper.getEndLabel(matchTraversal).get();
                    replaceStartAndEndSteps(matchTraversal);
                    break;
                }
            }
        }

        int insertIndex = TraversalHelper.stepIndex(matchStep, matchStep.getTraversal());
        for (Traversal.Admin<Object, Object> insertTraversal : orderedTraversals) {
            final Step<?, Object> insertAfterStep = TraversalHelper.insertTraversal(insertIndex, insertTraversal, matchStep.getTraversal());
            insertIndex = TraversalHelper.stepIndex(insertAfterStep, matchStep.getTraversal());
        }

        matchStep.getTraversal().removeStep(matchStep);
    }

    private static void replaceStartAndEndSteps(Traversal.Admin<Object, Object> matchTraversal) {
        final Step<Object,?> startStep = matchTraversal.getStartStep();
        final Step<?, Object> endStep = matchTraversal.getEndStep();
        final Step<?,?> newEndStep = endStep.getPreviousStep();

        assert(MatchStep.Helper.getEndLabel(matchTraversal).isPresent());
        String endLabel = MatchStep.Helper.getEndLabel(matchTraversal).get();

        matchTraversal.removeStep(startStep);
        matchTraversal.removeStep(endStep);
        newEndStep.addLabel(endLabel);
    }
}