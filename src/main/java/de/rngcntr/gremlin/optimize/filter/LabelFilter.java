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

public class LabelFilter<E extends Element> extends ElementFilter<E> {
    private final String label;

    public LabelFilter(Class<E> filteredType, String label) {
        super(filteredType);
        this.label = label;
    }

    public static <E1 extends Element> LabelFilter<E1> empty(Class<E1> filteredType) {
        return new LabelFilter<>(filteredType, null);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LabelFilter)) return false;
        LabelFilter<?> otherFilter = (LabelFilter<?>) other;
        return StringUtils.equals(label, otherFilter.label);
    }

    @Override
    public void applyTo(GraphTraversal<?,E> t) {
        t.hasLabel(label);
    }

    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return String.format("~label='%s'", label);
    }
}