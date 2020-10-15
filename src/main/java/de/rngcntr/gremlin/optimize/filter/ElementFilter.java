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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * @author Florian Grieskamp
 *
 * @param <E> The type of element, that this filter matches on.
 *            Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
 *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
 */
public abstract class ElementFilter<E extends Element> {
    protected Class<E> filteredType;

    /**
     * @param filteredType The type of element, that this filter matches on.
     *            Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     */
    public ElementFilter(Class<E> filteredType) {
        this.filteredType = filteredType;
    }

    /**
     * Returns the type of element, that this filter matches on.
     * @return Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     */
    public Class<E> getFilteredType() {
        return filteredType;
    }

    /**
     * Appends a filter step to the traversal, that realizes the behavior specified by this filter.
     * @param t The traversal to apply the filter on.
     */
    public abstract void applyTo(GraphTraversal<?,E> t);
}