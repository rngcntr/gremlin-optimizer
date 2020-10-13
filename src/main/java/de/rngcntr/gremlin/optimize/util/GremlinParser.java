package de.rngcntr.gremlin.optimize.util;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class GremlinParser {

    private GraphTraversal<?,?> traversal;
    private Stack<Step<?,?>> currentStepStack;
    private Stack<PatternElement<?>> currentElementStack;
    private List<PatternElement<?>> elements;
    private Map<String, PatternElement<?>> stepLabelMap;
    private Map<PatternElement<?>, String> elementsToReturn;

    public void parse(GraphTraversal<?,?> traversal) {
        this.traversal = traversal;
        elements = new ArrayList<>();
        stepLabelMap = new HashMap<>();
        currentStepStack = new Stack<>();
        elementsToReturn = new HashMap<>();
        currentElementStack = new Stack<>();

        currentElementStack.push(null);
        currentStepStack.push(traversal.asAdmin().getStartStep());

        while (!finished()) {
            Step<?,?> currentStep = advance();
            parseStep(currentStep);
        }

        assert !currentElementStack.isEmpty();
        PatternElement<?> lastElement = currentElementStack.pop();
        assert lastElement != null;
        if (elementsToReturn.isEmpty()) {
            // if no select step was parsed, return the last element
            elementsToReturn.put(lastElement, String.valueOf(lastElement.getId()));
        }
    }

    public boolean finished() {
        return currentStepStack.isEmpty();
    }

    public Step<?,?> advance() {
        Step<?,?> currentStep = currentStepStack.pop();
        Step<?,?> nextStep = currentStep.getNextStep();
        if (nextStep != EmptyStep.instance()) {
            currentStepStack.push(nextStep);
        }

        return currentStep;
    }

    public List<PatternElement<?>> getElements() {
        return elements;
    }

    public Map<PatternElement<?>, String> getElementsToReturn() {
        return elementsToReturn;
    }

    public void parseStep(Step<?,?> currentStep) {
        if (currentStep instanceof GraphStep<?,?>) {
            parseGraphStep((GraphStep<?, ?>) currentStep);
        } else if (currentElementStack.peek() == null) {
            throw new IllegalArgumentException("Traversal must start with GraphStep: " + traversal);
        } else if (currentStep instanceof HasStep<?>) {
            parseHasStep((HasStep<?>) currentStep);
        } else if (currentStep instanceof VertexStep<?>) {
            parseVertexStep((VertexStep<?>) currentStep);
        } else if (currentStep instanceof EdgeVertexStep) {
            parseEdgeStep((EdgeVertexStep) currentStep);
        } else if (currentStep instanceof SelectStep || currentStep instanceof SelectOneStep) {
            if (currentStep.getNextStep() != EmptyStep.instance()) {
                // TODO currently, only select steps at the end of the query are supported
                throw new IllegalArgumentException("Select steps are only allowed at the end of a query");
            }
            parseSelectStep((Scoping) currentStep);
        } else {
            throw new IllegalArgumentException("Unsupported step: " + currentStep);
        }

        for (String stepLabel : currentStep.getLabels()) {
            stepLabelMap.put(stepLabel, currentElementStack.peek());
        }
    }

    private void parseGraphStep(GraphStep<?,?> graphStep) {
        currentElementStack.pop();

        PatternElement<?> currentElement;
        if (graphStep.returnsVertex()) {
            currentElement = new PatternVertex();
        } else {
            currentElement = new PatternEdge();
        }

        elements.add(currentElement);
        currentElementStack.push(currentElement);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void parseHasStep(HasStep<?> hasStep) {
        PatternElement<?> currentElement = currentElementStack.pop();

        for (HasContainer hc : hasStep.getHasContainers()) {
            if (hc.getKey().equals(T.label.getAccessor())) {
                // assuming P.eq(label)
                LabelFilter filter = new LabelFilter<>(currentElement.getType(), (String) hc.getValue());
                currentElement.setLabelFilter(filter);
            } else {
                PropertyFilter filter = new PropertyFilter<>(currentElement.getType(), hc.getKey(), hc.getPredicate());
                currentElement.addPropertyFilter(filter);
            }
        }

        currentElementStack.push(currentElement);
    }

    @SuppressWarnings("unchecked")
    private void parseVertexStep(VertexStep<?> vertexStep) {
        if (vertexStep.returnsVertex()) {
            parseVertexVertexStep((VertexStep<Vertex>) vertexStep);
        } else {
            parseVertexEdgeStep((VertexStep<Edge>) vertexStep);
        }
    }

    private void parseVertexVertexStep(VertexStep<Vertex> vertexStep) {
        PatternVertex currentVertex = (PatternVertex) currentElementStack.pop();

        String edgeLabel = vertexStep.getEdgeLabels().length > 0 ? vertexStep.getEdgeLabels()[0] : null;
        Direction direction = vertexStep.getDirection();

        PatternEdge newEdge = createEdge(edgeLabel, currentVertex, direction);

        PatternVertex newVertex = new PatternVertex();
        newEdge.setVertex(newVertex, direction.opposite());
        newVertex.addEdge(newEdge, vertexStep.getDirection().opposite());

        elements.add(newEdge);
        elements.add(newVertex);
        currentElementStack.push(newVertex);
    }

    private void parseVertexEdgeStep(VertexStep<Edge> vertexStep) {
        PatternVertex currentVertex = (PatternVertex) currentElementStack.pop();

        String edgeLabel = vertexStep.getEdgeLabels().length > 0 ? vertexStep.getEdgeLabels()[0] : null;
        PatternEdge edge = createEdge(edgeLabel, currentVertex, vertexStep.getDirection());

        elements.add(edge);
        currentElementStack.push(edge);
    }

    private void parseSelectStep(Scoping selectStep) {
        PatternElement<?> currentElement = currentElementStack.pop();
        Set<String> selectedLabels = selectStep.getScopeKeys();
        selectedLabels.forEach(l -> {
            PatternElement<?> elem = stepLabelMap.get(l);
            if (elem == null) throw new IllegalArgumentException("Step label " + l + " is undefined");
            elementsToReturn.put(elem, l);
        });
        currentElementStack.push(currentElement);
    }

    private PatternEdge createEdge(String label, PatternVertex neighbor, Direction direction) {
        PatternEdge newEdge = new PatternEdge();

        if (label != null) {
            LabelFilter<Edge> edgeLabelFilter = new LabelFilter<>(Edge.class, label);
            newEdge.setLabelFilter(edgeLabelFilter);
        }

        neighbor.addEdge(newEdge, direction);
        newEdge.setVertex(neighbor, direction);

        return newEdge;
    }

    private void parseEdgeStep(EdgeVertexStep edgeStep) {
        PatternEdge currentEdge = (PatternEdge) currentElementStack.pop();

        Direction direction = edgeStep.getDirection();
        PatternVertex newVertex = new PatternVertex();

        currentEdge.setVertex(newVertex, direction);
        newVertex.addEdge(currentEdge, direction);

        elements.add(newVertex);
        currentElementStack.push(newVertex);
    }
}
