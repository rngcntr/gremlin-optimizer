package de.rngcntr.gremlin.optimize.util;

import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Collection;
import java.util.Map;

public class GremlinWriter {
    public static GraphTraversal<?, Map<String, Object>> applySelectStep(GraphTraversal<?,?> t, Collection<PatternElement<?>> elements) {
        String[] labelArray = elements.stream()
                .map(PatternElement::getId)
                .map(String::valueOf)
                .toArray(String[]::new);
        if (labelArray.length == 0) {
            return t.select(""); // TODO undefined behavior
        } else if (labelArray.length == 1) {
            return t.select(labelArray[0]);
        } else {
            String[] remainingLabelArray = new String[labelArray.length - 2];
            System.arraycopy(labelArray, 2, remainingLabelArray, 0, remainingLabelArray.length);
            return t.select(labelArray[0], labelArray[1], remainingLabelArray);
        }
    }
}
