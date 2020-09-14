package de.rngcntr.gremlin.optimize.retrieval.direct;

import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.structure.Element;

public abstract class DirectRetrieval<E extends Element>  extends Retrieval<E> {
    public DirectRetrieval(Class<E> retrievedType) {
        super(retrievedType);
    }

    @Override
    public void estimate(StatisticsProvider stats) {
        // if filters are available, use the most selective
        Long estimateByProperties = element.getPropertyFilters().stream()
                .map(f -> f.estimateSelectivity(stats))
                .min(Long::compareTo)
                .orElse(stats.totals(retrievedType));
        long estimateByLabel = element.hasLabelFilter()
                ? element.getLabelFilter().estimateSelectivity(stats)
                : IMPOSSIBLE;
        this.estimatedSize = Math.min(estimateByProperties, estimateByLabel);
    }
}
