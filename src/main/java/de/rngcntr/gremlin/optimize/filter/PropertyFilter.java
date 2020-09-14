package de.rngcntr.gremlin.optimize.filter;

import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

public class PropertyFilter<E extends Element> extends ElementFilter<E> {
    private final String key;
    private final P<?> predicate;

    public PropertyFilter(Class<E> filteredType, String key, P<?> predicate) {
        super(filteredType);
        this.key = key;
        this.predicate = predicate;
    }

    @Override
    public void applyTo(GraphTraversal<?,E> t) {
        t.has(key, predicate);
    }

    @Override
    public long estimateSelectivity(StatisticsProvider stats) {
        return stats.withProperty(this);
    }
}
