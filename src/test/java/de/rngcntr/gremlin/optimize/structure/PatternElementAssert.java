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

package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.*;

public class PatternElementAssert {

    /**
     * Checks the existence and identity of a label filter on a pattern element.
     *
     * @param element The element to be filtered.
     * @param labels Can be either zero or one label string.
     * @param <E> Vertex or Edge.
     */
    public static <E extends Element> void assertLabelsExactlyMatch(PatternElement<E> element, String... labels) {
        if (labels.length == 0) {
            assertFalse(element.hasLabelFilter());
        } else {
            assertTrue(element.hasLabelFilter());
            LabelFilter<E> labelFilter = element.getLabelFilter();
            for (String label : labels) {
                assertEquals(label, labelFilter.getLabel());
            }
        }
    }

    public static <E extends Element> void assertPropertiesExactlyMatch(PatternElement<E> element, String... keyValues) {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Key without value: " + keyValues[keyValues.length-1]);
        }

        assertEquals(keyValues.length / 2, element.getPropertyFilters().size());
        for (int i = 0; i < keyValues.length; ++i) {
            assertTrue(containsFilter(element, keyValues[i], keyValues[++i]));
        }
    }

    public static <E extends Element> boolean containsFilter(PatternElement<E> element, String key, String value) {
        Collection<PropertyFilter<E>> properties = element.getPropertyFilters();
        for (PropertyFilter<E> p : properties) {
            if (p.getKey().equals(key) && p.getPredicate().equals(P.eq(value))) {
                return true;
            }
        }
        return false;
    }

    public static void assertVertexCount(PatternGraph pg, long expectedNumVertices) {
        assertEquals(expectedNumVertices, pg.getVertices().size());
    }

    public static void assertEdgeCount(PatternGraph pg, long expectedNumEdges) {
        assertEquals(expectedNumEdges, pg.getEdges().size());
    }

    public static void assertVertexAndEdgeCount(PatternGraph pg, long numExpectedVertices, long numExpectedEdges) {
        assertVertexCount(pg, numExpectedVertices);
        assertEdgeCount(pg, numExpectedEdges);
    }
}