package de.rngcntr.gremlin.optimize.retrieval.dependent;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Optional;

public abstract class DependentRetrieval<E extends Element>  extends Retrieval<E> {
    protected PatternElement<?> source;
    protected Direction direction;

    public DependentRetrieval(Class<E> retrievedType) {
        super(retrievedType);
    }

    public PatternElement<?> getSource() {
        return source;
    }

    @Override
    public void estimate(StatisticsProvider stats) {
        if (isSelfDependent()) {
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
                long absoluteConnections = direction == Direction.OUT
                        ? stats.connections(element.getLabelFilter(), source.getLabelFilter())
                        : stats.connections(source.getLabelFilter(), element.getLabelFilter());
                labelSelectivity = (double) absoluteConnections / stats.withLabel(source.getLabelFilter());
            } else {
                // calculate #{e* -> vLabel} / #{e*}
                long absoluteConnections = direction == Direction.OUT
                        ? stats.connections(element.getLabelFilter(), LabelFilter.empty(source.getType()))
                        : stats.connections(LabelFilter.empty(source.getType()), element.getLabelFilter());
                labelSelectivity = (double) absoluteConnections / stats.totals(source.getType());
            }
        }

        /*
            determine selectivity of property filter
         */

        double filterSelectivity = 1.0;
        if (element.hasLabelFilter()) {
            long total = stats.withLabel(element.getLabelFilter());
            Optional<Long> totalFiltered = element.getPropertyFilters().stream()
                    .map(f -> stats.withProperty(element.getLabelFilter(), f))
                    .min(Long::compareTo);
            filterSelectivity = (double) totalFiltered.orElse(total) / total;
        }

        /*
            combine to estimation
         */
        estimatedSize = (long) (incomingSize * labelSelectivity * filterSelectivity);
    }

    @Override
    public String toString() {
        return String.format("via %d, Estimation: ~%d", source.getId(), estimatedSize);
    }

    private boolean isSelfDependent() {
        Retrieval<?> r = this;
        while (r instanceof DependentRetrieval) {
            PatternElement<?> source = ((DependentRetrieval<?>) r).getSource();
            if (element.equals(source)) {
                return true;
            }
            r = source.getBestRetrieval();
        }

        return false;
    }
}
