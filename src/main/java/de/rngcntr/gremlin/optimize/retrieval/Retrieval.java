package de.rngcntr.gremlin.optimize.retrieval;

import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

public abstract class Retrieval<E extends Element> implements Comparable<Retrieval<?>>{
    protected final Class<E> retrievedType;
    protected long estimatedSize;
    protected GraphTraversal.Admin<?,E> t;
    protected PatternElement<E> element;

    public static final Long IMPOSSIBLE = Long.MAX_VALUE;

    public Retrieval(Class<E> retrievedType) {
        this.retrievedType = retrievedType;
        t = new DefaultGraphTraversal<>();
        estimatedSize = IMPOSSIBLE;
    }

    public abstract void estimate(StatisticsProvider stats);

    public long getEstimatedSize() {
        return estimatedSize;
    }

    @Override
    public int compareTo(Retrieval<?> other) {
        return Long.compare(this.estimatedSize, other.estimatedSize);
    }
}
