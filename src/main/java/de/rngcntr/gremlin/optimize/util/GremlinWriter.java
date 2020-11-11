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

import de.rngcntr.gremlin.optimize.query.DependencyTree;
import de.rngcntr.gremlin.optimize.query.Join;
import de.rngcntr.gremlin.optimize.query.PartialQueryPlan;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.step.JoinStep;
import de.rngcntr.gremlin.optimize.strategy.FlattenMatchStepStrategy;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.structure.PatternGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.*;

public class GremlinWriter {

    public static GraphTraversal<?,?> buildTraversal(PatternGraph pg) {
        Set<PatternElement<?>> directlyRetrieved = new HashSet<>();
        Set<DependencyTree> dependencyTrees = new HashSet<>();

        // find all directly retrievable elements
        pg.getElements().stream()
                .filter(elem -> elem.getBestRetrieval() instanceof DirectRetrieval)
                .forEach(directlyRetrieved::add);

        // build pattern matching queries for dependent retrievals
        for (PatternElement<?> baseElement : directlyRetrieved) {
            final DependencyTree dependencyTree = DependencyTree.of(baseElement.getBestRetrieval());
            dependencyTrees.add(dependencyTree);
        }

        GraphTraversal<Map<String,Object>,Map<String,Object>> completeTraversal = joinTraversals(dependencyTrees, pg.getSourceGraph());

        final GraphTraversal<?, Map<String, Object>> assembledTraversal = GremlinWriter.selectLabels(completeTraversal, pg.getElementsToReturn());
        FlattenMatchStepStrategy.instance().apply(assembledTraversal.asAdmin());
        return assembledTraversal;
    }

    private static GraphTraversal<Map<String,Object>,Map<String,Object>> joinTraversals(Set<DependencyTree> dependencyTrees, Graph g) {
        Iterator<DependencyTree> depTreeIterator = dependencyTrees.iterator();
        assert depTreeIterator.hasNext();
        PartialQueryPlan leftSide = depTreeIterator.next();
        while (depTreeIterator.hasNext()) {
            PartialQueryPlan rightSide = depTreeIterator.next();
            leftSide = new Join(leftSide, rightSide);
        }
        System.out.println(leftSide);
        final GraphTraversal<Map<String,Object>,Map<String,Object>> joinedTraversal = leftSide.asTraversal();
        joinedTraversal.asAdmin().setGraph(g);
        return joinedTraversal;
    }

    public static GraphTraversal<Map<String,Object>, Map<String, Object>> selectElements(GraphTraversal<Map<String,Object>,?> t, Collection<PatternElement<?>> elements, boolean alwaysMap) {
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

    private static GraphTraversal<Map<String,Object>, Map<String, Object>> selectLabels(GraphTraversal<Map<String,Object>,Map<String,Object>> t, Map<PatternElement<?>, String> mappedElements) {
        List<PatternElement<?>> elements = new ArrayList<>(mappedElements.keySet());
        String[] externalLabels = elements.stream()
                .map(mappedElements::get)
                .toArray(String[]::new);

        GraphTraversal<Map<String,Object>, Map<String, Object>> projectedTraversal;

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