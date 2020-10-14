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

package de.rngcntr.gremlin.optimize.retrieval;

import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
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
        GraphTraversal<?,E> t = getBaseTraversal();

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