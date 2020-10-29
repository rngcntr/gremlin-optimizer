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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static de.rngcntr.gremlin.optimize.testutils.structure.PatternElementAssert.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class PatternGraphTests {

    private GraphTraversalSource g;

    @BeforeEach
    public void initializeGraph() {
        Graph graph = mock(Graph.class);
        g = new GraphTraversalSource(graph);
    }

    @Test
    public void testVertexAndEdgeCreation() {
        assertVertexAndEdgeCount(new PatternGraph(g.V()), 1, 0);
        assertVertexAndEdgeCount(new PatternGraph(g.E()), 0, 1);

        // with labels
        assertVertexAndEdgeCount(new PatternGraph(g.V().hasLabel("label")), 1, 0);
        assertVertexAndEdgeCount(new PatternGraph(g.E().hasLabel("label")), 0, 1);

        // with properties
        assertVertexAndEdgeCount(new PatternGraph(g.V().has("key", "value")), 1, 0);
        assertVertexAndEdgeCount(new PatternGraph(g.E().has("key", "value")), 0, 1);

        // with steps
        assertVertexAndEdgeCount(new PatternGraph(g.V().outE()), 1, 1);
        assertVertexAndEdgeCount(new PatternGraph(g.V().out()), 2, 1);
        assertVertexAndEdgeCount(new PatternGraph(g.V().inE()), 1, 1);
        assertVertexAndEdgeCount(new PatternGraph(g.V().in()), 2, 1);

        assertVertexAndEdgeCount(new PatternGraph(g.E().inV()), 1, 1);
        assertVertexAndEdgeCount(new PatternGraph(g.E().outV()), 1, 1);
    }

    @Test
    public void testLabelAssignment() {
        GraphTraversal<?,?> t = g.V();
        assertLabelsExactlyMatch(new PatternGraph(t).getVertices().get(0));

        t = g.V().hasLabel("label");
        assertLabelsExactlyMatch(new PatternGraph(t).getVertices().get(0), "label");

        t = g.V().hasLabel("label").out();
        assertLabelsExactlyMatch(new PatternGraph(t).getVertices().get(0), "label");
        assertLabelsExactlyMatch(new PatternGraph(t).getVertices().get(1));

        t = g.V().hasLabel("label").out("label1").hasLabel("label2");
        assertLabelsExactlyMatch(new PatternGraph(t).getVertices().get(0), "label");
        assertLabelsExactlyMatch(new PatternGraph(t).getVertices().get(1), "label2");
        assertLabelsExactlyMatch(new PatternGraph(t).getEdges().get(0), "label1");

        t = g.V().outE("label1");
        assertLabelsExactlyMatch(new PatternGraph(t).getVertices().get(0));
        assertLabelsExactlyMatch(new PatternGraph(t).getEdges().get(0), "label1");
    }

    @Test
    public void testPropertyAssignment() {
        GraphTraversal<?,?> t = g.V();
        assertPropertiesExactlyMatch(new PatternGraph(t).getVertices().get(0));

        t = g.V().has("key", "value");
        assertPropertiesExactlyMatch(new PatternGraph(t).getVertices().get(0), "key", "value");

        t = g.V().has("key", "value").out();
        assertPropertiesExactlyMatch(new PatternGraph(t).getVertices().get(0), "key", "value");
        assertPropertiesExactlyMatch(new PatternGraph(t).getVertices().get(1));
        assertPropertiesExactlyMatch(new PatternGraph(t).getEdges().get(0));

        t = g.V().has("key", "value").out("label").has("key2", "value2");
        assertPropertiesExactlyMatch(new PatternGraph(t).getVertices().get(0), "key", "value");
        assertPropertiesExactlyMatch(new PatternGraph(t).getVertices().get(1), "key2", "value2");
        assertPropertiesExactlyMatch(new PatternGraph(t).getEdges().get(0));

        t = g.V().outE("label1").has("key", "value");
        assertPropertiesExactlyMatch(new PatternGraph(t).getVertices().get(0));
        assertPropertiesExactlyMatch(new PatternGraph(t).getEdges().get(0), "key", "value");
    }

    @Test
    public void testUnsupportedStepRaisesException() {
        GraphTraversal<?,?> t = g.V().limit(1);
        assertThrows(IllegalArgumentException.class, () -> new PatternGraph(t));
    }

    @Test
    public void testSelectStep() {
        assertThrows(IllegalArgumentException.class, () -> new PatternGraph(g.V().out().select("a")));
        assertThrows(IllegalArgumentException.class, () -> new PatternGraph(g.V().out().select("a", "b")));
        assertDoesNotThrow(() -> new PatternGraph(g.V().as("a").out().as("b").select("a")));
        assertDoesNotThrow(() -> new PatternGraph(g.V().as("a").out().as("b").select("b")));
        assertDoesNotThrow(() -> new PatternGraph(g.V().as("a").out().as("b").select("a", "b")));
    }

    @Test
    public void testExampleFromAnimation() {
        PatternGraph basePattern = new PatternGraph(g.V()
                .hasLabel("store").as("s")
                .out("belongs_to").hasLabel("company").has("name", "Apple").select("s")
                .in("buys_at").hasLabel("customer").has("name", "Bob").select("s")
                .out("located_in").hasLabel("country"));

        List<PatternGraph> otherPatterns = new ArrayList<>();
        otherPatterns.add(new PatternGraph(g.V()
                .hasLabel("store").as("s")
                .in("buys_at").hasLabel("customer").has("name", "Bob").select("s")
                .out("belongs_to").hasLabel("company").has("name", "Apple").select("s")
                .out("located_in").hasLabel("country")));
        otherPatterns.add(new PatternGraph(g.V()
                .hasLabel("customer")
                .has("name", "Bob")
                .out("buys_at")
                .hasLabel("store").as("s")
                .out("belongs_to").hasLabel("company").has("name", "Apple").select("s")
                .out("located_in").hasLabel("country")));
        otherPatterns.add(new PatternGraph(g.V()
                .hasLabel("company")
                .has("name", "Apple")
                .in("belongs_to")
                .hasLabel("store").as("s")
                .in("buys_at").hasLabel("customer").has("name", "Bob").select("s")
                .out("located_in").hasLabel("country")));
        otherPatterns.add(new PatternGraph(g.V()
                .hasLabel("country")
                .as("returnValue")
                .in("located_in")
                .hasLabel("store").as("s")
                .in("buys_at").hasLabel("customer").has("name", "Bob").select("s")
                .out("belongs_to").hasLabel("company").has("name", "Apple").select("s")
                .select("returnValue")
        ));

        for (PatternGraph otherPattern : otherPatterns) {
            assertEquals(basePattern, otherPattern);
        }
    }
}