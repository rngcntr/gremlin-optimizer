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

import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.step.JoinStep;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.structure.PatternGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.*;

public class GremlinWriter {

    public static GraphTraversal<?,?> buildTraversal(PatternGraph pg) {
        Set<PatternElement<?>> directlyRetrieved = new HashSet<>();
        Set<GraphTraversal<Map<String,Object>,Map<String,Object>>> joinedTraversals = new HashSet<>();

        /*
         * find all directly retrievable elements
         */
        pg.getElements().stream()
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
            Queue<PatternElement<?>> dependencyQueue = new LinkedList<>(baseElement.getDependentNeighbors());
            if (baseElement.isEdge()) {
                // If the base element is an edge and does not have dependent neighbors on both ends,
                // the open ends should be added to the pattern, too (for joining)
                for (PatternElement<?> neighbor : baseElement.getNeighbors(Direction.BOTH)) {
                    if (baseElement.getDependentNeighbors().contains(neighbor)) {continue;}
                    assert neighbor.getDependentRetrieval(baseElement).isPresent();
                    matchTraversals.add(neighbor.getDependentRetrieval(baseElement).get().asTraversal());
                    elementsToBeSelected.add(neighbor);
                }
            }
            while (!dependencyQueue.isEmpty()) {
                PatternElement<?> dependentElement = dependencyQueue.poll();
                elementsToBeSelected.add(dependentElement);
                assert dependentElement.getBestDependentRetrieval().isPresent();
                matchTraversals.add(dependentElement.getBestDependentRetrieval().get().asTraversal());
                List<PatternElement<?>> dependentNeighbors = dependentElement.getDependentNeighbors();
                if (dependentNeighbors.isEmpty() && dependentElement.isEdge()) {
                    // pattern groups may not end in open edges
                    // => add their adjacent vertices to the pattern group
                    for (PatternElement<?> neighbor : dependentElement.getNeighbors(Direction.BOTH)) {
                        if (elementsToBeSelected.contains(neighbor)) {continue;}
                        assert neighbor.getDependentRetrieval(dependentElement).isPresent();
                        matchTraversals.add(neighbor.getDependentRetrieval(dependentElement).get().asTraversal());
                        elementsToBeSelected.add(neighbor);
                    }
                } else {
                    dependencyQueue.addAll(dependentNeighbors);
                }
            }

            // assemble Gremlin match traversal
            GraphTraversal<Map<String,Object>,?> assembledTraversal = baseElement.getBestRetrieval().asTraversal();
            if (matchTraversals.size() > 0) {
                assembledTraversal = assembledTraversal.match(matchTraversals.toArray(new GraphTraversal[0]));
            }
            joinedTraversals.add(GremlinWriter.selectElements(assembledTraversal, elementsToBeSelected, true));
        }

        Iterator<GraphTraversal<Map<String,Object>,Map<String,Object>>> joinedTraversalIterator = joinedTraversals.iterator();
        assert joinedTraversalIterator.hasNext();
        GraphTraversal<Map<String,Object>,?> completeTraversal = joinedTraversalIterator.next();
        completeTraversal.asAdmin().setGraph(pg.getSourceGraph());
        while (joinedTraversalIterator.hasNext()) {
            completeTraversal.asAdmin().addStep(new JoinStep(completeTraversal.asAdmin(), joinedTraversalIterator.next()));
        }
        return GremlinWriter.selectLabels(completeTraversal, pg.getElementsToReturn());
    }

    private static GraphTraversal<Map<String,Object>, Map<String, Object>> selectElements(GraphTraversal<Map<String,Object>,?> t, Collection<PatternElement<?>> elements, boolean alwaysMap) {
        String[] internalLabels = elements.stream()
                .map(PatternElement::getId)
                .map(String::valueOf)
                .toArray(String[]::new);
        if (elements.size() == 0) {
            return t.select(""); // TODO undefined behavior
        } else if (elements.size() == 1) {
            return alwaysMap
                    ? t.select(internalLabels[0], internalLabels[0])
                    : t.select(internalLabels[0]);
        } else {
            String[] remainingInternalLabels = new String[internalLabels.length - 2];
            System.arraycopy(internalLabels, 2, remainingInternalLabels, 0, remainingInternalLabels.length);
            return t.select(internalLabels[0], internalLabels[1], remainingInternalLabels);
        }
    }

    private static GraphTraversal<?, Map<String, Object>> selectLabels(GraphTraversal<Map<String,Object>,?> t, Map<PatternElement<?>, String> mappedElements) {
        List<PatternElement<?>> elements = new ArrayList<>(mappedElements.keySet());
        String[] externalLabels = elements.stream()
                .map(mappedElements::get)
                .toArray(String[]::new);

        GraphTraversal<?, Map<String, Object>> projectedTraversal;

        /*
         * apply project step
         */
        if (elements.size() < 2) {
            // nothing needs to be mapped, just select the element
            return selectElements(t, elements, false);
        } else {
            String[] remainingExternalLabels = new String[externalLabels.length - 1];
            System.arraycopy(externalLabels, 1, remainingExternalLabels, 0, remainingExternalLabels.length);
            projectedTraversal = t.project(externalLabels[0], remainingExternalLabels);
        }

        /*
         * apply by(select())... steps
         */
        for (PatternElement<?> element : elements) {
            projectedTraversal = projectedTraversal.by(__.select(String.valueOf(element.getId())));
        }

        return projectedTraversal;
    }
}