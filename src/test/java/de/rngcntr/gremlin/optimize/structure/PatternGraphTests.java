package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static de.rngcntr.gremlin.optimize.structure.PatternElementAssert.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    public void testGenerateTraversal() {
        GraphTraversal<?,?> t = g.V().outE("knows");
        PatternGraph pg = new PatternGraph(t);
        StatisticsProvider stats = mock(StatisticsProvider.class);
        when(stats.totals(Vertex.class)).thenReturn(1L);
        when(stats.totals(Edge.class)).thenReturn(3L);
        when(stats.withLabel(any())).thenReturn(2L);
        pg.optimize(stats);
        // TODO execute actual test
    }

    @Test
    public void testExampleFromAnimation() {
        GraphTraversal<?,?> t = g.V()
                .hasLabel("store")
                .where(__.out("belongs_to").has("name", "Apple"))
                .where(__.in("buys_at").has("name", "Bob"))
                .out("located_in");
        PatternGraph pg = new PatternGraph(t);
    }
}
