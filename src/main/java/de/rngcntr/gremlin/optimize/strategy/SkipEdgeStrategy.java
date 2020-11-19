package de.rngcntr.gremlin.optimize.strategy;

import de.rngcntr.gremlin.optimize.query.JoinAttribute;
import de.rngcntr.gremlin.optimize.step.JoinStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class SkipEdgeStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final SkipEdgeStrategy INSTANCE = new SkipEdgeStrategy();

    public static SkipEdgeStrategy instance() {
        return INSTANCE;
    }

    @Override
    public Set<Class<? extends OptimizationStrategy>> applyPrior() {
        return new HashSet(Arrays.asList(RemoveUnusedLabelsStrategy.class, RemoveRedundantSelectStrategy.class));
    }

    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClassRecursively(EdgeVertexStep.class, traversal).forEach(evs -> {
            Direction direction = evs.getDirection().opposite();
            Step<?,?> previousStep = evs.getPreviousStep();
            Set<String> edgeLabels = new HashSet<>();
            HasStep<?> hasStep = null;
            if (previousStep instanceof HasStep) {
                hasStep = (HasStep<?>) previousStep;
                List<HasContainer> hasContainers = ((HasStep) previousStep).getHasContainers();
                if (hasContainers.stream().anyMatch(hc -> !hc.getKey().equals(T.label.getAccessor()))) {
                    return;
                } else {
                   hasContainers.forEach(hc -> edgeLabels.add((String) hc.getValue()));
                }
                previousStep = previousStep.getPreviousStep();
            }
            if (previousStep instanceof VertexStep) {
                VertexStep<?> previousVertexStep = (VertexStep<?>) previousStep;
                if (previousVertexStep.returnsEdge() && previousVertexStep.getDirection().equals(direction)) {
                    VertexStep<Edge> newVertexStep = new VertexStep(traversal, Vertex.class, direction, edgeLabels.toArray(new String[edgeLabels.size()]));
                    TraversalHelper.insertAfterStep(newVertexStep, previousVertexStep.getPreviousStep(), traversal);
                    traversal.removeStep(previousVertexStep);
                    traversal.removeStep(evs);
                    if (hasStep != null) {
                        traversal.removeStep(hasStep);
                    }
                }
            }
        });
    }
}
