package de.rngcntr.gremlin.optimize.filter;

import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

public class LabelFilter<E extends Element> extends ElementFilter<E> {
    private final String label;

    public LabelFilter(Class<E> filteredType, String label) {
        super(filteredType);
        this.label = label;
    }

    public static <E1 extends Element> LabelFilter<E1> empty(Class<E1> filteredType) {
        return new LabelFilter<>(filteredType, null);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LabelFilter)) return false;
        LabelFilter<?> otherFilter = (LabelFilter<?>) other;
        return StringUtils.equals(label, otherFilter.label);
    }

    @Override
    public void applyTo(GraphTraversal<?,E> t) {
        t.hasLabel(label);
    }

    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return String.format("~label='%s'", label);
    }
}
