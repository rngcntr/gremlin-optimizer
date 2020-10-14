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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PropertyFilterTests {

    @ParameterizedTest
    @CsvSource({
            "testKey0, 0",
            "testKey1, 1"
    })
    public void testConstructor(String key, int value) {
        P<?> p = P.eq(value);

        PropertyFilter<?> vertexFilter = new PropertyFilter<>(Vertex.class, key, p);
        PropertyFilter<?> edgeFilter = new PropertyFilter<>(Edge.class, key, p);

        assertEquals(key, vertexFilter.getKey());
        assertEquals(p, vertexFilter.getPredicate());
        assertEquals(Vertex.class, vertexFilter.getFilteredType());

        assertEquals(key, edgeFilter.getKey());
        assertEquals(p, edgeFilter.getPredicate());
        assertEquals(Edge.class, edgeFilter.getFilteredType());

    }
}