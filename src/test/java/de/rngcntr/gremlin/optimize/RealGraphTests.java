package de.rngcntr.gremlin.optimize;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

public class RealGraphTests {
    private GraphTraversalSource g;

    @BeforeEach
    public void initializeGraph() {
        Graph graph = TinkerFactory.createModern();
        g = graph.traversal();
    }

    public void assertSameResultAfterOptimization(GraphTraversal<?,?> traversal, StatisticsProvider stats, int numExpectedResults) {
        GraphTraversal<?,?> unoptimizedTraversal = traversal.asAdmin().clone();
        Multiset<?> unoptimizedResults = HashMultiset.create(unoptimizedTraversal.toList());
        PatternGraph pg = new PatternGraph(traversal);
        GraphTraversal<?,?> optimizedTraversal = pg.optimize(stats);
        Multiset<?> optimizedResults = HashMultiset.create(optimizedTraversal.toList());
        Assertions.assertEquals(numExpectedResults, unoptimizedResults.size());
        Assertions.assertEquals(numExpectedResults, optimizedResults.size());
        Assertions.assertEquals(unoptimizedResults, optimizedResults);
    }

    private static Stream<Arguments> testedTraversals() {
        return Stream.of(
                Arguments.of(6, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()),
                Arguments.of(1, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                        .has("name", "marko")),
                Arguments.of(4, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                        .hasLabel("person")),
                Arguments.of(2, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                        .hasLabel("person").has("age", P.lt(30))),
                Arguments.of(2, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                        .hasLabel("person").has("name", "josh")
                        .outE("created")),
                Arguments.of(1, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                        .hasLabel("person")
                        .outE("created").has("weight", 1.0)),
                Arguments.of(0, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                        .hasLabel("person")
                        .out("created")
                        .hasLabel("person")),
                Arguments.of(1, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                        .hasLabel("person").has("name", "josh")
                        .out("created")
                        .hasLabel("software")
                        .in("created")
                        .hasLabel("person").has("name", "marko")),
                Arguments.of(1, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                        .hasLabel("person").has("name", "josh")
                        .out("created")
                        .hasLabel("software").as("a")
                        .in("created")
                        .hasLabel("person").has("name", "marko")
                        .select("a")),
                Arguments.of(1, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                        .hasLabel("person").has("name", "josh")
                        .out("created").as("a")
                        .hasLabel("software")
                        .in("created").as("b")
                        .hasLabel("person").has("name", "marko")
                        .select("a", "b"))
        );
    }

    @ParameterizedTest
    @MethodSource("testedTraversals")
    public void testTraversalWithDefaultStatistics(int numExpectedResults, Function<GraphTraversalSource, GraphTraversal<?,?>> t){
        StatisticsProvider stats = mock(StatisticsProvider.class);
        assertSameResultAfterOptimization(t.apply(g), stats, numExpectedResults);
    }
}
