package de.rngcntr.gremlin.optimize.filter;

import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LabelFilterTests {

    @ParameterizedTest
    @ValueSource(strings = {
            "testLabel0",
            "testLabel1"
    })
    public void testConstructor(String testLabel) {
        LabelFilter<?> filter = new LabelFilter<>(Vertex.class, testLabel);
        assertEquals(testLabel, filter.getLabel());
    }

    @Test
    public void testEmptyConstructor() {
        LabelFilter<?> filter = LabelFilter.empty(Vertex.class);
        assertEquals(null, filter.getLabel());
    }

    @ParameterizedTest
    @CsvSource({
            "testLabel0, 5",
            "testLabel0, 7",
            "testLabel1, 5"
    })
    public void testSelectivityEstimation(String testLabel, long testAmount) {
        StatisticsProvider statMock = mock(StatisticsProvider.class);
        LabelFilter<?> filter = new LabelFilter<>(Vertex.class, testLabel);
        when(statMock.withLabel(filter)).thenReturn(testAmount);

        assertEquals(testAmount, filter.estimateSelectivity(statMock));
    }
}
