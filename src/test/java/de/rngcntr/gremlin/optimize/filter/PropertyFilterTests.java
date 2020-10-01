package de.rngcntr.gremlin.optimize.filter;

import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @ParameterizedTest
    @CsvSource({
            "testKey0, 5",
            "testKey0, 7",
            "testKey1, 5"
    })
    public void testSelectivityEstimation(String testKey, long testAmount) {
        StatisticsProvider statMock = mock(StatisticsProvider.class);
        PropertyFilter<?> filter = new PropertyFilter<>(Vertex.class, testKey, P.eq(0));
        when(statMock.withProperty(filter)).thenReturn(testAmount);

        assertEquals(testAmount, filter.estimateSelectivity(statMock));
    }
}
