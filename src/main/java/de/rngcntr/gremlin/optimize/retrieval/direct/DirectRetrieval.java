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

package de.rngcntr.gremlin.optimize.retrieval.direct;

import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * @author Florian Grieskamp
 *
 * A DirectRetrieval specifies that candidates matching a {@link de.rngcntr.gremlin.optimize.structure.PatternElement}
 * are retrieved directly from the storage and independently of connected elements by using a global gremlin query.
 *
 * @param <E> The type of element that is retrieved.
 *            Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
 *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
 */
public abstract class DirectRetrieval<E extends Element>  extends Retrieval<E> {
    /**
     * Creates a direct retrieval and estimates it as impossible.
     */
    public DirectRetrieval() {
        super();
    }

    /**
     * Updates the estimation for this retrieval based on the provided statistics.
     * If the retrieved element has no label constraint, the absolute estimation of elements with the same type is used.
     * Else, the most selective property or label constraint is used.
     *
     * @param stats The statistics provider that is used.
     */
    @Override
    public void estimate(StatisticsProvider stats) {
        // if filters are available, use the most selective
        if (!getElement().hasLabelFilter()) {
            this.estimatedSize = stats.totals(getElement().getType());
        } else {
            Long estimateByProperties = getElement().getPropertyFilters().stream()
                    .map(f -> stats.withProperty(getElement().getLabelFilter(), f))
                    .min(Long::compareTo)
                    .orElse(IMPOSSIBLE);
            long estimateByLabel = stats.withLabel(getElement().getLabelFilter());
            this.estimatedSize = Math.min(estimateByProperties, estimateByLabel);
        }

    }

    /**
     * Represents this retrieval as a human readable text.
     *
     * @return The text representation of this retrieval.
     */
    @Override
    public String toString() {
        return String.format("%d Direct, Estimation: ~%d", getElement().getId(), estimatedSize);
    }
}