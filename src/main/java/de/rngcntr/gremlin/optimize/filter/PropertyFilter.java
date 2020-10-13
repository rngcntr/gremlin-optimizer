package de.rngcntr.gremlin.optimize.filter;

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

    public String getKey() {
        return key;
    }

    public P<?> getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return String.format("%s=%s", key, predicate);
    }
}
