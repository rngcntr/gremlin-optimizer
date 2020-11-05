// Copyright 2020 Florian Grieskamp
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package de.rngcntr.gremlin.optimize.retrieval.dependent;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Optional;

/**
 * @author Florian Grieskamp
 *
 * A DependentRetrieval specifies that candidates matching a {@link de.rngcntr.gremlin.optimize.structure.PatternElement}
 * are retrieved depending on already retrieved neighbors using a local child of a gremlin match query.
 *
 * @param <E> The type of element that is retrieved.
 *            Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
 *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
 */
public abstract class DependentRetrieval<E extends Element>  extends Retrieval<E> {
    /**
     * The pattern element that the retrieved element depends on.
     */
    protected PatternElement<?> source;

    /**
     * The direction of the dependency.
     */
    protected Direction direction;

    /**
     * Creates a dependent retrieval and estimates it as impossible.
     */
    public DependentRetrieval() {
        super();
    }

    /**
     * Gets the pattern element that the retrieved element depends on.
     *
     * @return The source pattern element.
     */
    public PatternElement<?> getSource() {
        return source;
    }

    /**
     * Gets the direction of the dependency.
     *
     * @return The direction of the dependency.
     */
    public Direction getDirection() {
        return direction;
    }

    /**
     * Updates the estimation for this retrieval based on the provided statistics.
     * If the retrieval is self dependent, it is considered impossible, otherwise the estimation of the source element's
     * best retrieval is used as a basis, to which the selectivities of label and property constraints are applied.
     *
     * @param stats The statistics provider that is used.
     */
    @Override
    public void estimate(StatisticsProvider stats) {
        if (isSelfDependent()) {
            estimatedSize = IMPOSSIBLE;
            return;
        }

        long incomingSize = getSource().getBestRetrieval().getEstimatedSize();

        /*
            determine selectivity of label filter
         */
        double labelSelectivity;
        if (getElement().hasLabelFilter()) {
            if (getSource().hasLabelFilter()) {
                // calculate #{eLabel -> vLabel} / #{eLabel}
                long absoluteConnections = getDirection() == Direction.OUT
                        ? stats.connections(getElement().getLabelFilter(), getSource().getLabelFilter())
                        : stats.connections(getSource().getLabelFilter(), getElement().getLabelFilter());
                labelSelectivity = (double) absoluteConnections / stats.withLabel(getSource().getLabelFilter());
            } else {
                // calculate #{e* -> vLabel} / #{e*}
                long absoluteConnections = getDirection() == Direction.OUT
                        ? stats.connections(getElement().getLabelFilter(), LabelFilter.empty(getSource().getType()))
                        : stats.connections(LabelFilter.empty(getSource().getType()), getElement().getLabelFilter());
                labelSelectivity = (double) absoluteConnections / stats.totals(getSource().getType());
            }
        } else {
            if (getSource().hasLabelFilter()) {
                long absoluteConnections = getDirection() == Direction.OUT
                        ? stats.connections(LabelFilter.empty(getElement().getType()), getSource().getLabelFilter())
                        : stats.connections(getSource().getLabelFilter(), LabelFilter.empty((getElement().getType())));
                labelSelectivity = (double) absoluteConnections / stats.withLabel(getSource().getLabelFilter());
            } else {
                long absoluteConnections = getDirection() == Direction.OUT
                        ? stats.connections(LabelFilter.empty(getElement().getType()), LabelFilter.empty(getSource().getType()))
                        : stats.connections(LabelFilter.empty(getSource().getType()), LabelFilter.empty((getElement().getType())));
                labelSelectivity = (double) absoluteConnections / stats.totals(getSource().getType());
            }
        }

        /*
            determine selectivity of property filter
         */

        double filterSelectivity = 1.0;
        if (getElement().hasLabelFilter()) {
            long total = stats.withLabel(getElement().getLabelFilter());
            Optional<Long> totalFiltered = getElement().getPropertyFilters().stream()
                    .map(f -> stats.withProperty(getElement().getLabelFilter(), f))
                    .min(Long::compareTo);
            filterSelectivity = (double) totalFiltered.orElse(total) / total;
        }

        /*
            combine to estimation
         */
        estimatedSize = (long) Math.ceil(incomingSize * labelSelectivity * filterSelectivity);
    }

    /**
     * Represents this retrieval as a human readable text.
     *
     * @return The text representation of this retrieval.
     */
    @Override
    public String toString() {
        return String.format("via %d, Estimation: ~%d", getSource().getId(), estimatedSize);
    }

    /**
     * Determines whether or not the chain of source elements contains a dependency cycle or not, i. e. if at the
     * current state, the best way to retrieve this element, depends on it being already retrieved.
     *
     * @return <ul>
     *     <li><code>true</code> if the retrieval depends on itself.</li>
     *     <li><code>false</code> otherwise.</li>
     * </ul>
     */
    public boolean isSelfDependent() {
        Retrieval<?> r = this;
        while (r instanceof DependentRetrieval) {
            PatternElement<?> otherSource = ((DependentRetrieval<?>) r).getSource();
            if (getElement().equals(otherSource)) {
                return true;
            }
            r = otherSource.getBestRetrieval();
        }

        return false;
    }
}