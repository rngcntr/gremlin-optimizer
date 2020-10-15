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

package de.rngcntr.gremlin.optimize.testutils.strategy;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import de.rngcntr.gremlin.optimize.testutils.step.UniqueEdgeCountStep;
import de.rngcntr.gremlin.optimize.testutils.step.UniqueVertexCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UniqueElementCountStrategyTest {
    private GraphTraversalSource g;

    @BeforeEach
    public void initializeGraph() {
        Graph graph = TinkerFactory.createModern();
        g = graph.traversal();
    }

    public void assertSameResultAfterOptimization(GraphTraversal<?,?> traversal) {
        GraphTraversal<?,?> unoptimizedTraversal = traversal.asAdmin().clone();
        Multiset<?> unoptimizedResults = HashMultiset.create(unoptimizedTraversal.toList());
        TraversalStrategies strategies = TraversalStrategies.GlobalCache.getStrategies(Graph.class);
        strategies.addStrategies(new UniqueElementCountStrategy());
        traversal.asAdmin().setStrategies(strategies);
        Multiset<?> optimizedResults = HashMultiset.create(traversal.toList());
        assertEquals(unoptimizedResults, optimizedResults);
    }

    private static Stream<Arguments> testedTraversals() {
        return Stream.of(
                Arguments.of(6, 0, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()),
                Arguments.of(6, 0, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                                .has("name", "marko")),
                Arguments.of(6, 0, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                                .hasLabel("person")),
                Arguments.of(6, 2, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                                .hasLabel("person").has("name", "josh")
                                .outE("created")),
                Arguments.of(6, 4, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                                .hasLabel("person")
                                .outE("created").has("weight", 1.0)),
                Arguments.of(6, 4, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                                .hasLabel("person")
                                .out("created")),
                Arguments.of(6, 4, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                                .hasLabel("person").has("name", "josh")
                                .out("created")
                                .hasLabel("software")
                                .in("created")),
                Arguments.of(6, 2, (Function<GraphTraversalSource, GraphTraversal<?,?>>) g ->
                        g.V()
                                .hasLabel("person").has("name", "vadas")
                                .in("knows")
                                .out("created"))
        );
    }

    @ParameterizedTest
    @MethodSource("testedTraversals")
    public void testTraversal(int numVertices, int numEdges, Function<GraphTraversalSource, GraphTraversal<?,?>> t){
        UniqueEdgeCountStep.resetCount();
        UniqueVertexCountStep.resetCount();
        GraphTraversal<?,?> traversal = t.apply(g);
        assertSameResultAfterOptimization(traversal);
        assertEquals(numEdges, UniqueEdgeCountStep.getCount());
        assertEquals(numVertices, UniqueVertexCountStep.getCount());
    }
}
