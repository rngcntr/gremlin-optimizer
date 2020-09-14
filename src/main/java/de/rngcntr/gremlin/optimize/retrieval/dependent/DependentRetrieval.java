package de.rngcntr.gremlin.optimize.retrieval.dependent;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Optional;

public abstract class DependentRetrieval<E extends Element>  extends Retrieval<E> {
    protected PatternElement<?> source;

    public DependentRetrieval(Class<E> retrievedType) {
        super(retrievedType);
    }

    @Override
    public void estimate(StatisticsProvider stats) {
        if (!source.isFinal()) {
            estimatedSize = IMPOSSIBLE;
            return;
        }

        long incomingSize = source.getBestRetrieval().getEstimatedSize();

        /*
            determine selectivity of label filter
         */
        double labelSelectivity = 1.0;
        if (element.hasLabelFilter()) {
            if (source.hasLabelFilter()) {
                // calculate #{eLabel -> vLabel} / #{eLabel}
                labelSelectivity = (double) stats.connections(source.getLabelFilter(), element.getLabelFilter())
                        / stats.withLabel(source.getLabelFilter());
            } else {
                // calculate #{e* -> vLabel} / #{e*}
                labelSelectivity = (double) stats.connections(LabelFilter.empty(source.getType()), element.getLabelFilter())
                        / stats.totals(source.getType());
            }
        }

        /*
            determine selectivity of property filter
         */
        long total = stats.totals(retrievedType);
        Optional<Long> totalFiltered = element.getPropertyFilters().stream().map(f -> f.estimateSelectivity(stats)).min(Long::compareTo);
        double filterSelectivity = (double) totalFiltered.orElse(stats.totals(retrievedType)) / total;

        /*
            combine to estimation
         */
        estimatedSize = (long) (incomingSize * labelSelectivity * filterSelectivity);
    }
}
