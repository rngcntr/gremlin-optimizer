package de.rngcntr.gremlin.optimize.filter;

import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
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
    public void applyTo(GraphTraversal<?,E> t) {
        t.hasLabel(label);
    }

    public String getLabel() {
        return label;
    }

    @Override
    public long estimateSelectivity(StatisticsProvider stats) {
        return stats.withLabel(this);
    }
}
