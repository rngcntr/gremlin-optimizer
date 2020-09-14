package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;
import java.util.stream.Collectors;

public class PatternGraph {
    private final Collection<PatternElement<?>> elements;

    public PatternGraph(GraphTraversal<?,?> t) {
        elements = new ArrayList<>();
        buildGraphFromTraversal(t);
    }

    private void buildGraphFromTraversal(GraphTraversal<?,?> t) {
        PatternElement<?> currentElement = null;
        for (Step<?, ?> currentStep = t.asAdmin().getStartStep();
             currentStep != EmptyStep.instance();
             currentStep = currentStep.getNextStep()) {
            if (currentStep instanceof HasStep<?>) {
                parseHasStep((HasStep<?>) currentStep, currentElement);
            } else if (currentStep instanceof VertexStep<?>) {
                List<PatternElement<?>> newElements = parseVertexStep((VertexStep<?>) currentStep, (PatternVertex) currentElement);
                elements.addAll(newElements);
                currentElement = newElements.get(0);
            } else if (currentStep instanceof EdgeVertexStep) {
                currentElement = parseEdgeStep((EdgeVertexStep) currentStep, (PatternEdge) currentElement);
                elements.add(currentElement);
            }
        }
    }

    private static <E extends Element> void parseHasStep(HasStep<?> hasStep, PatternElement<E> currentElement) {
        if (currentElement == null) {
            return;
        }

        for (HasContainer hc : hasStep.getHasContainers()) {
            if (hc.getKey().equals(T.label.getAccessor())) {
                // assuming P.eq(label)
                LabelFilter<E> filter = new LabelFilter(currentElement.getType(), (String) hc.getValue());
                currentElement.setLabelFilter(filter);
            } else {
                PropertyFilter<E> filter = new PropertyFilter(currentElement.getType(), hc.getKey(), hc.getPredicate());
                currentElement.addPropertyFilter(filter);
            }
        }
    }

    private static List<PatternElement<?>> parseVertexStep(VertexStep<?> vertexStep, PatternVertex currentElement) {
        if (vertexStep.returnsVertex()) {
            return parseVertexVertexStep((VertexStep<Vertex>) vertexStep, currentElement);
        } else {
            return parseVertexEdgeStep((VertexStep<Edge>) vertexStep, currentElement);
        }
    }

    private static List<PatternElement<?>> parseVertexVertexStep(VertexStep<Vertex> vertexStep, PatternVertex currentVertex) {
        String edgeLabel = vertexStep.getEdgeLabels().length > 0 ? vertexStep.getEdgeLabels()[0] : null;
        Direction direction = vertexStep.getDirection();

        PatternEdge newEdge = createEdge(edgeLabel, currentVertex, direction);

        PatternVertex newVertex = new PatternVertex();
        newEdge.setVertex(newVertex, direction.opposite());
        newVertex.addEdge(newEdge, vertexStep.getDirection().opposite());

        return Arrays.asList(newVertex, newEdge);
    }

    private static List<PatternElement<?>> parseVertexEdgeStep(VertexStep<Edge> vertexStep, PatternVertex currentVertex) {
        String edgeLabel = vertexStep.getEdgeLabels().length > 0 ? vertexStep.getEdgeLabels()[0] : null;
        return Arrays.asList(createEdge(edgeLabel, currentVertex, vertexStep.getDirection()));
    }

    private static PatternEdge createEdge(String label, PatternVertex neighbor, Direction direction) {
        PatternEdge newEdge = new PatternEdge();

        if (label != null) {
            LabelFilter<Edge> edgeLabelFilter = new LabelFilter<>(Edge.class, label);
            newEdge.setLabelFilter(edgeLabelFilter);
        }

        neighbor.addEdge(newEdge, direction);
        newEdge.setVertex(neighbor, direction);

        return newEdge;
    }

    private static PatternVertex parseEdgeStep(EdgeVertexStep edgeStep, PatternEdge currentEdge) {
        Direction direction = edgeStep.getDirection();
        PatternVertex newVertex = new PatternVertex();

        currentEdge.setVertex(newVertex, direction);
        newVertex.addEdge(currentEdge, direction);

        return newVertex;
    }

    public GraphTraversal<?,?> optimize(StatisticsProvider stats) {
        // 1st step: initialization of the graph and estimation of direct retrievals
        elements.forEach(PatternElement::initializeRetrievals);
        elements.forEach(e -> e.estimateDirectRetrievals(stats));

        // 2nd step: mark most selective PatternElement as final
        ArrayList<PatternElement<?>> leftElements = new ArrayList<>(elements);
        PatternElement<?> mostSelective = Collections.min(leftElements);
        leftElements.remove(mostSelective);
        mostSelective.makeFinal();

        // n-th step
        while (!leftElements.isEmpty()) {
            Collection<PatternElement<?>> neighbors = mostSelective.getNeighbors();
            neighbors.removeIf(PatternElement::isFinal);
            neighbors.forEach(e -> e.estimateDependentRetrievals(stats));
            mostSelective = Collections.min(neighbors);
            leftElements.remove(mostSelective);
            mostSelective.makeFinal();
        }

        return buildTraversal();
    }

    private GraphTraversal<?,?> buildTraversal() {
        // TODO
        return new DefaultGraphTraversal<>();
    }

    @Override
    public String toString() {
        return elements.stream().map(PatternElement::toString)
                .collect(Collectors.joining("\n", "Graph consisting of:\n", ""));
    }
}
