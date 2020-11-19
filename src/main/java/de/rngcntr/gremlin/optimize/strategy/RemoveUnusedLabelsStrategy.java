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
        return new HashSet(Arrays.asList(RemoveRedundantSelectStrategy.class));
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        Set<String> selectedLabels = getSelectedLabels(traversal);
        TraversalHelper.getStepsOfAssignableClassRecursively(Step.class, traversal).forEach(s -> {
            Set<String> stepLabels = new HashSet<String>(s.getLabels());
            for (String stepLabel : stepLabels) {
                if (selectedLabels.stream().noneMatch(sl -> stepLabel.equals(sl))) {
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
