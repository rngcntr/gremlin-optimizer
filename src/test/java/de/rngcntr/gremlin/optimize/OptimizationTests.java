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

import de.rngcntr.gremlin.optimize.statistics.MockedStatUtils;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import de.rngcntr.gremlin.optimize.structure.PatternGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

public class OptimizationTests {

    private GraphTraversalSource g;

    @BeforeEach
    public void initializeGraph() {
        Graph graph = EmptyGraph.instance();
        g = new GraphTraversalSource(graph);
    }

    @Test
    public void testExampleFromAnimation() {
        StatisticsProvider stats = mockStatsFromAnimation();
        PatternGraph pg = new PatternGraph(g.V()
                .hasLabel("store")
                .where(__.out("belongs_to").hasLabel("company").has("name", "Apple"))
                .where(__.in("buys_at").hasLabel("customer").has("name", "Bob"))
                .out("located_in").hasLabel("country"));

        pg.optimize(stats);

        // TODO: check traversal
    }

    private StatisticsProvider mockStatsFromAnimation() {
        StatisticsProvider stats = mock(StatisticsProvider.class);
        MockedStatUtils.withTotalEstimation(stats, Vertex.class, 111_200L);
        MockedStatUtils.withTotalEstimation(stats, Edge.class, 2_020_000L);
        MockedStatUtils.withLabelEstimation(stats, "store", 10_000L);
        MockedStatUtils.withLabelEstimation(stats, "country", 200L);
        MockedStatUtils.withLabelEstimation(stats, "customer", 100_000L);
        MockedStatUtils.withLabelEstimation(stats, "company", 1_000L);
        MockedStatUtils.withLabelEstimation(stats, "located_in", 10_000L);
        MockedStatUtils.withLabelEstimation(stats, "buys_at", 2_000_000L);
        MockedStatUtils.withLabelEstimation(stats, "belongs_to", 10_000L);
        MockedStatUtils.withPropertyEstimation(stats, "customer", "name", 100L);
        MockedStatUtils.withPropertyEstimation(stats, "company", "name", 1L);
        MockedStatUtils.withConnectivityEstimation(stats, "located_in", "country", 10_000L);
        MockedStatUtils.withConnectivityEstimation(stats, "store", "located_in", 10_000L);
        MockedStatUtils.withConnectivityEstimation(stats, "belongs_to", "company", 10_000L);
        MockedStatUtils.withConnectivityEstimation(stats, "store", "belongs_to", 10_000L);
        MockedStatUtils.withConnectivityEstimation(stats, "buys_at", "store", 2_000_000L);
        MockedStatUtils.withConnectivityEstimation(stats, "customer", "buys_at", 2_000_000L);
        return stats;
    }
}