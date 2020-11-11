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
        System.out.println(pg);
        GraphTraversal<?,?> optimizedTraversal = pg.optimize(stats);
        printStepwiseResults(optimizedTraversal.asAdmin().clone());
        Multiset<?> optimizedResults = HashMultiset.create(optimizedTraversal.toList());
        System.out.println(optimizedResults);
        Assertions.assertEquals(numExpectedResults, unoptimizedResults.size());
        Assertions.assertEquals(numExpectedResults, optimizedResults.size());
        Assertions.assertEquals(unoptimizedResults, optimizedResults);
    }

    private void printStepwiseResults(GraphTraversal<?, ?> traversal) {
        while (!traversal.asAdmin().getSteps().isEmpty()) {
            System.out.println(traversal.asAdmin().getEndStep());
            try {
                System.out.println(traversal.asAdmin().clone().toList());
            } catch (Exception ignored) {}
            traversal.asAdmin().removeStep(traversal.asAdmin().getSteps().size() - 1);
        }
    }

    private static Stream<Arguments> testedTraversals() {
        return Stream.of(
                Arguments.of(2, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V().has("name", "marko").as("a").out("knows").as("b").select("a").select("a", "b")),
                Arguments.of(1, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V().has("name", "marko").as("a").out("created").as("b").select("a").select("a", "b")),
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
                                .select("a", "b")),
                Arguments.of(2, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                                .hasLabel("person").as("a")
                                .outE("knows").select("a")),
                Arguments.of(4, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                                .hasLabel("person")
                                .out("created")
                                .hasLabel("software"))
        );
    }

    @ParameterizedTest
    @MethodSource("testedTraversals")
    public void testTraversalWithDefaultStatistics(int numExpectedResults, Function<GraphTraversalSource, GraphTraversal<?,?>> t){
        StatisticsProvider stats = mock(StatisticsProvider.class);
        assertSameResultAfterOptimization(t.apply(g), stats, numExpectedResults);
    }
}