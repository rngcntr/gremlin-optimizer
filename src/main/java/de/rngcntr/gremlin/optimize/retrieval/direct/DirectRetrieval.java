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