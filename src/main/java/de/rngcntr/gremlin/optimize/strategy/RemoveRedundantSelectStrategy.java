package de.rngcntr.gremlin.optimize.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public class RemoveRedundantSelectStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final RemoveRedundantSelectStrategy INSTANCE = new RemoveRedundantSelectStrategy();

    public static RemoveRedundantSelectStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClassRecursively(SelectOneStep.class, traversal).forEach(s -> {
            if (s.getScopeKeys().size() == 1) {
                String key = (String) s.getScopeKeys().iterator().next();
                Step previousStep = s.getPreviousStep();
                if (previousStep.getLabels().stream().anyMatch(l -> l.equals(key))) {
                    traversal.removeStep(s);
                }
            }
        });
    }
}
