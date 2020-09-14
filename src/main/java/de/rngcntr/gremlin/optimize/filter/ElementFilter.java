package de.rngcntr.gremlin.optimize.filter;

import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

public abstract class ElementFilter<E extends Element> {
    protected Class<E> filteredType;

    public ElementFilter(Class<E> filteredType) {
        this.filteredType = filteredType;
    }

    public Class<E> getFilteredType() {
        return filteredType;
    }

    public abstract void applyTo(GraphTraversal<?,E> t);

    public abstract long estimateSelectivity(StatisticsProvider stats);
}
