package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.step.JoinStep;
import de.rngcntr.gremlin.optimize.util.GremlinParser;
import de.rngcntr.gremlin.optimize.util.GremlinWriter;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;

import java.util.*;
import java.util.stream.Collectors;

public class PatternGraph {
    private final List<PatternElement<?>> elements;
    private final Map<String, PatternElement<?>> stepLabelMap;
    private final Set<PatternElement<?>> elementsToReturn;

    public PatternGraph(GraphTraversal<?,?> t) {
        elements = new ArrayList<>();
        stepLabelMap = new HashMap<>();
        elementsToReturn = new HashSet<>();
        buildGraphFromTraversal(t);
    }

    private void buildGraphFromTraversal(GraphTraversal<?,?> t) {
        PatternElement<?> currentElement = null;
        for (Step<?, ?> currentStep = t.asAdmin().getStartStep();
             currentStep != EmptyStep.instance();
             currentStep = currentStep.getNextStep()) {

            if (currentStep instanceof GraphStep<?,?>) {
                currentElement = GremlinParser.parseGraphStep((GraphStep<?, ?>) currentStep);
                elements.add(currentElement);
            } else if (currentElement == null) {
                throw new IllegalArgumentException("Traversal must start with GraphStep: " + t);
            } else if (currentStep instanceof HasStep<?>) {
                GremlinParser.parseHasStep((HasStep<?>) currentStep, currentElement);
            } else if (currentStep instanceof VertexStep<?>) {
                List<PatternElement<?>> newElements = GremlinParser.parseVertexStep((VertexStep<?>) currentStep, (PatternVertex) currentElement);
                elements.addAll(newElements);
                currentElement = newElements.get(0);
            } else if (currentStep instanceof EdgeVertexStep) {
                currentElement = GremlinParser.parseEdgeStep((EdgeVertexStep) currentStep, (PatternEdge) currentElement);
                elements.add(currentElement);
            } else if (currentStep instanceof SelectStep || currentStep instanceof SelectOneStep) {
                if (currentStep.getNextStep() != EmptyStep.instance()) {
                    // TODO currently, only select steps at the end of the query are supported
                    throw new IllegalArgumentException("Select steps are only allowed at the end of a query");
                }
                Set<String> selectedLabels = GremlinParser.parseSelectStep((Scoping) currentStep);
                selectedLabels.forEach(l -> {
                    PatternElement<?> elem = stepLabelMap.get(l);
                    if (elem == null) throw new IllegalArgumentException("Step label " + l + " is undefined");
                    elementsToReturn.add(elem);
                });
            } else {
                throw new IllegalArgumentException("Unsupported step: " + currentStep);
            }

            for (String stepLabel : currentStep.getLabels()) {
                stepLabelMap.put(stepLabel, currentElement);
            }
        }

        assert currentElement != null;
        if (elementsToReturn.isEmpty()) {
            // if no select step was parsed, return the last element
            elementsToReturn.add(currentElement);
        }
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
        Set<PatternElement<?>> directlyRetrieved = new HashSet<>();
        Set<GraphTraversal<?,Map<String,Object>>> joinedTraversals = new HashSet<>();

        /*
         * find all directly retrievable elements
         */
        elements.stream()
                .filter(elem -> elem.getBestRetrieval() instanceof DirectRetrieval)
                .forEach(directlyRetrieved::add);

        /*
         * build pattern matching queries for dependent retrievals
         */
        for (PatternElement<?> baseElement : directlyRetrieved) {
            Set<GraphTraversal<?,?>> matchTraversals = new HashSet<>();
            Set<PatternElement<?>> elementsToBeSelected = new HashSet<>();
            elementsToBeSelected.add(baseElement);
            // run BFS to find neighbors dependent on this element
            Queue<PatternElement<?>> elementQueue = new LinkedList<>(baseElement.getNeighbors());
            while (!elementQueue.isEmpty()) {
                PatternElement<?> dependentElement = elementQueue.poll();
                elementsToBeSelected.add(dependentElement);
                matchTraversals.add(dependentElement.getBestRetrieval().asTraversal());
                dependentElement.getNeighbors().stream()
                        .map(PatternElement::getBestRetrieval)
                        .filter(retrieval -> retrieval instanceof DependentRetrieval)
                        .map(retrieval -> (DependentRetrieval<?>) retrieval)
                        .filter(retrieval -> retrieval.getSource() == dependentElement)
                        .map(DependentRetrieval::getElement)
                        .forEach(elementQueue::add);
            }

            GraphTraversal<?,?> assembledTraversal = baseElement.getBestRetrieval().asTraversal();
            if (matchTraversals.size() > 0) {
                assembledTraversal = assembledTraversal.match(matchTraversals.toArray(new GraphTraversal[0]));
            }
            joinedTraversals.add(GremlinWriter.applySelectStep(assembledTraversal, elementsToBeSelected));
        }

        Iterator<GraphTraversal<?,Map<String,Object>>> joinedTraversalIterator = joinedTraversals.iterator();
        assert joinedTraversalIterator.hasNext();
        GraphTraversal<?,?> completeTraversal = joinedTraversalIterator.next();
        while (joinedTraversalIterator.hasNext()) {
            completeTraversal.asAdmin().addStep(new JoinStep(completeTraversal.asAdmin(), joinedTraversalIterator.next()));
        }
        return GremlinWriter.applySelectStep(completeTraversal, elementsToReturn);
    }

    public List<PatternVertex> getVertices() {
        return elements.stream()
                .filter(e -> e instanceof PatternVertex)
                .map(e -> (PatternVertex) e)
                .collect(Collectors.toList());
    }

    public List<PatternEdge> getEdges() {
        return elements.stream()
                .filter(e -> e instanceof PatternEdge)
                .map(e -> (PatternEdge) e)
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return elements.stream().map(PatternElement::toString)
                .collect(Collectors.joining("\n", "Graph consisting of:\n", ""));
    }
}
