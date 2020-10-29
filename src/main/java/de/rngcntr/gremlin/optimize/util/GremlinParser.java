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
    }

    private boolean finished() {
        return currentStepStack.isEmpty();
    }

    private Step<?,?> advance() {
        Step<?,?> currentStep = currentStepStack.pop();

        if (currentStep instanceof EmptyStep) {
            PatternElement<?> currentElement = currentElementStack.pop();
            if (currentStepStack.isEmpty()) {
                // end of traversal
                assert currentElement != null;
                if (elementsToReturn.isEmpty()) {
                    // if no select step was parsed, return the last element
                    elementsToReturn.put(currentElement, String.valueOf(currentElement.getId()));
                }
            }
        } else {
            currentStepStack.push(currentStep.getNextStep());
        }

        return currentStep;
    }

    public List<PatternElement<?>> getElements() {
        return elements;
    }

    public Map<PatternElement<?>, String> getElementsToReturn() {
        return elementsToReturn;
    }

    private void parseStep(Step<?,?> currentStep) {
        if (currentStep instanceof EmptyStep) {
            return;
        } else if (currentStep instanceof GraphStep<?,?>) {
            parseGraphStep((GraphStep<?, ?>) currentStep);
        } else if (currentElementStack.peek() == null) {
            throw new IllegalArgumentException("Traversal must start with GraphStep: " + traversal);
        } else if (currentStep instanceof HasStep<?>) {
            parseHasStep((HasStep<?>) currentStep);
        } else if (currentStep instanceof VertexStep<?>) {
            parseVertexStep((VertexStep<?>) currentStep);
        } else if (currentStep instanceof EdgeVertexStep) {
            parseEdgeStep((EdgeVertexStep) currentStep);
        } else if (currentStep instanceof SelectOneStep) {
            parseSelectStep((SelectOneStep<?,?>) currentStep);
        } else if (currentStep instanceof SelectStep) {
            parseSelectStep((SelectStep<?,?>) currentStep);
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

    private void parseSelectStep(SelectOneStep<?,?> selectStep) {
        currentElementStack.pop();
        assert selectStep.getScopeKeys().size() == 1;
        String selectedLabel = selectStep.getScopeKeys().iterator().next();
        PatternElement<?> elem = stepLabelMap.get(selectedLabel);
        if (elem == null) throw new IllegalArgumentException("Step label " + selectedLabel + " is undefined");
        currentElementStack.push(elem);
    }

    private void parseSelectStep(SelectStep<?,?> selectStep) {
        if ((Step<?,?>) selectStep.getNextStep() != EmptyStep.instance()) {
            // TODO currently, selecting multiple step labels is only supported at the end of a query
            throw new IllegalArgumentException("Selecting multiple labels is only allowed at the end of a query");
        }
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