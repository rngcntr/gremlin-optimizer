package de.rngcntr.gremlin.optimize.util;

import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.structure.PatternGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;

import java.util.*;

public class GremlinWriter {
    public static GraphTraversal<?, Map<String, Object>> selectElements(GraphTraversal<?,?> t, Collection<PatternElement<?>> elements, boolean alwaysMap) {
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

    public static GraphTraversal<?, Map<String, Object>> selectLabels(GraphTraversal<?,?> t, Map<PatternElement<?>, String> mappedElements) {
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
