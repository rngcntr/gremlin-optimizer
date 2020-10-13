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
        if (!element.hasLabelFilter()) {
            this.estimatedSize = stats.totals(retrievedType);
        } else {
            Long estimateByProperties = element.getPropertyFilters().stream()
                    .map(f -> stats.withProperty(element.getLabelFilter(), f))
                    .min(Long::compareTo)
                    .orElse(IMPOSSIBLE);
            long estimateByLabel = stats.withLabel(element.getLabelFilter());
            this.estimatedSize = Math.min(estimateByProperties, estimateByLabel);
        }

    }

    @Override
    public String toString() {
        return String.format("Direct, Estimation: ~%d", estimatedSize);
    }
}
