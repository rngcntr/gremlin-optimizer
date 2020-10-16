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

/**
 * @author Florian Grieskamp
 *
 * A Retrieval specifies how candidates matching a {@link PatternElement} can be collected from the graph storage.
 * This can happen either directly via an {@link de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval} by
 * considering all existing elements of the matching type (that is {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
 * {@link org.apache.tinkerpop.gremlin.structure.Edge}) or indirectly via an
 * {@link de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval} that considers only neighbors of a
 * previously retrieved element.
 *
 * @param <E> The type of element that is retrieved.
 *            Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
 *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
 *
 * @see de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval
 * @see de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval
 */
public abstract class Retrieval<E extends Element> implements Comparable<Retrieval<?>>{
    /**
     * The most recent result size estimation for this retrieval.
     */
    protected long estimatedSize;

    /**
     * The pattern element that defines a pattern for matching elements in the graph.
     */
    protected PatternElement<E> element;

    /**
     * If a retrieval is not executable, it's expected result size is assigned this value.
     */
    public static final Long IMPOSSIBLE = Long.MAX_VALUE;

    /**
     * Creates a retrieval and estimates it as impossible.
     */
    public Retrieval() {
        estimatedSize = IMPOSSIBLE;
    }

    /**
     * Updates the estimation for this retrieval based on the provided statistics.
     *
     * @param stats The statistics provider that is used.
     */
    public abstract void estimate(StatisticsProvider stats);

    /**
     * Generates a traversal that retrieves all candidates for the pattern element without applying the element's filters.
     *
     * @return The Gremlin traversal.
     */
    protected abstract GraphTraversal<?,E> getBaseTraversal();

    /**
     * Generates a traversal that retrieves all candidates for the pattern element and also applies the element's
     * filters to it.
     *
     * @return The complete traversal.
     */
    public GraphTraversal<?,E> asTraversal() {
        GraphTraversal<?,E> t = getBaseTraversal();

        if (getElement().hasLabelFilter()) {
            getElement().getLabelFilter().applyTo(t);
        }

        for (PropertyFilter<E> propertyFilter : getElement().getPropertyFilters()) {
            propertyFilter.applyTo(t);
        }

        return t.as(String.valueOf(getElement().getId()));
    }

    /**
     * Gets the latest estimation of this retrieval's result size.
     *
     * @return The estimated result size.
     */
    public long getEstimatedSize() {
        return estimatedSize;
    }

    /**
     * Compares itself to another retrieval based on their estimated sizes.
     *
     * @param other The other retrieval.
     * @return <ul>
     *     <li>A positive integer if this retrieval is expected to return more elements than the other retrieval.</li>
     *     <li>0 if this retrieval is expected to return exactly as many elements as the other retrieval.</li>
     *     <li>A negative integer if this retrieval is expected to return fewer elements than the other retrieval.</li>
     * </ul>
     */
    @Override
    public int compareTo(Retrieval<?> other) {
        return Long.compare(this.getEstimatedSize(), other.getEstimatedSize());
    }

    /**
     * Gets the pattern element that this retrieval tries to match on.
     *
     * @return The pattern element.
     */
    public PatternElement<E> getElement() {
        return element;
    }
}