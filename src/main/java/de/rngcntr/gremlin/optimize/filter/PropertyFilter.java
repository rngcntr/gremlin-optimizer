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

package de.rngcntr.gremlin.optimize.filter;

import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * @author Florian Grieskamp
 *
 * A property filter specifies a the existence of a property on a pattern element, which matches a given predicate.
 *
 * @param <E> The type of element, that this filter matches on.
 *            Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
 *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
 */
public class PropertyFilter<E extends Element> extends ElementFilter<E> {
    private final String key;
    private final P<?> predicate;

    /**
     * Creates a property constraint for a pattern element.
     *
     * @param filteredType Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     * @param key The name of the property.
     * @param predicate The predicate which should be enforced for the property.
     */
    public PropertyFilter(Class<E> filteredType, String key, P<?> predicate) {
        super(filteredType);
        this.key = key;
        this.predicate = predicate;
    }

    /**
     * Compares itself with another object for equality.
     *
     * @param other The other object.
     * @return <ul>
     *     <li><code>true</code>, if the other object is a property filter on an equal key and with an equal predicate</li>
     *     <li><code>false</code> otherwise.</li>
     * </ul>
     */
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PropertyFilter)) return false;
        if (getFilteredType() != ((PropertyFilter<?>) other).getFilteredType()) return false;
        PropertyFilter<?> otherFilter = (PropertyFilter<?>) other;
        if (!StringUtils.equals(key, otherFilter.key)) return false;
        if (predicate == null) return otherFilter.predicate == null;
        return predicate.equals(((PropertyFilter<?>) other).predicate);
    }

    @Override
    public void applyTo(GraphTraversal<?,E> t) {
        t.has(key, predicate);
    }

    /**
     * Gets the key of the property.
     *
     * @return The key.
     */
    public String getKey() {
        return key;
    }

    /**
     * Gets the predicate of the property.
     *
     * @return The predicate.
     */
    public P<?> getPredicate() {
        return predicate;
    }

    /**
     * Represents this filter as a human readable text.
     *
     * @return A text representation of the filter.
     */
    @Override
    public String toString() {
        return String.format("%s=%s", key, predicate);
    }
}