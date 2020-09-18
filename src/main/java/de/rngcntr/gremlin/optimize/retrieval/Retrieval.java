package de.rngcntr.gremlin.optimize.retrieval;

import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

public abstract class Retrieval<E extends Element> implements Comparable<Retrieval<?>>{
    protected final Class<E> retrievedType;
    protected long estimatedSize;
    protected PatternElement<E> element;

    public static final Long IMPOSSIBLE = Long.MAX_VALUE;

    public Retrieval(Class<E> retrievedType) {
        this.retrievedType = retrievedType;
        estimatedSize = IMPOSSIBLE;
    }

    public abstract void estimate(StatisticsProvider stats);

    protected abstract GraphTraversal<?,E> getBaseTraversal();

    public GraphTraversal<?,E> asTraversal() {
        GraphTraversal t = getBaseTraversal();

        if (element.hasLabelFilter()) {
            element.getLabelFilter().applyTo(t);
        }

        for (PropertyFilter<E> propertyFilter : element.getPropertyFilters()) {
            propertyFilter.applyTo(t);
        }

        return t.as(String.valueOf(element.getId()));
    }

    public long getEstimatedSize() {
        return estimatedSize;
    }

    @Override
    public int compareTo(Retrieval<?> other) {
        return Long.compare(this.estimatedSize, other.estimatedSize);
    }

    public PatternElement<E> getElement() {
        return element;
    }
}
