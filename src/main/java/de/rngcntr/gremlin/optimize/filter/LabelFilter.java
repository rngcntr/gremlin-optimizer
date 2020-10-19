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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * @author Florian Grieskamp
 *
 * A label filter specifies a the existence of a label with a given value for a pattern element.
 *
 * @param <E> The type of element, that this filter matches on.
 *            Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
 *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
 */
public class LabelFilter<E extends Element> extends ElementFilter<E> {
    private final String label;

    /**
     * Creates a label constraint for a pattern element.
     *
     * @param filteredType Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     * @param label The label which should be enforced for an element.
     */
    public LabelFilter(Class<E> filteredType, String label) {
        super(filteredType);
        this.label = label;
    }

    /**
     * Explicitly states that an element has no label constraint.
     *
     * @param filteredType Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     * @param <E> The type of element, that this filter matches on.
     *            Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     * @return A static singleton instance of the empty filter object.
     */
    public static <E extends Element> LabelFilter<E> empty(Class<E> filteredType) {
        return new LabelFilter<>(filteredType, null);
    }

    /**
     * Compares itself with another object for equality.
     *
     * @param other The other object.
     * @return <ul>
     *     <li><code>true</code>, if the other object is a label filter on an equal label</li>
     *     <li><code>false</code> otherwise.</li>
     * </ul>
     */
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LabelFilter)) return false;
        LabelFilter<?> otherFilter = (LabelFilter<?>) other;
        return getFilteredType() == ((LabelFilter<?>) other).getFilteredType()
                && StringUtils.equals(label, otherFilter.label);
    }

    @Override
    public void applyTo(GraphTraversal<?,E> t) {
        t.hasLabel(label);
    }

    /**
     * Gets the label of the filter.
     * @return The label.
     */
    public String getLabel() {
        return label;
    }

    /**
     * Represents this filter as a human readable text.
     *
     * @return A text representation of the filter.
     */
    @Override
    public String toString() {
        return String.format("~label='%s'", label);
    }
}