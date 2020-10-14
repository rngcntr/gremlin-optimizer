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

public class PropertyFilter<E extends Element> extends ElementFilter<E> {
    private final String key;
    private final P<?> predicate;

    public PropertyFilter(Class<E> filteredType, String key, P<?> predicate) {
        super(filteredType);
        this.key = key;
        this.predicate = predicate;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PropertyFilter)) return false;
        PropertyFilter<?> otherFilter = (PropertyFilter<?>) other;
        if (!StringUtils.equals(key, otherFilter.key)) return false;
        if (predicate == null) return otherFilter.predicate == null;
        return predicate.equals(((PropertyFilter<?>) other).predicate);
    }

    @Override
    public void applyTo(GraphTraversal<?,E> t) {
        t.has(key, predicate);
    }

    public String getKey() {
        return key;
    }

    public P<?> getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return String.format("%s=%s", key, predicate);
    }
}