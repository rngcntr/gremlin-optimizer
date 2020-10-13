package de.rngcntr.gremlin.optimize.statistics;

import org.apache.tinkerpop.gremlin.structure.Element;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

public class MockedStatUtils {

    public static <E extends Element> void withTotalEstimation(StatisticsProvider stats, Class<E> clazz, long estimation) {
        when(stats.totals(
                argThat(c -> c != null && c.equals(clazz))
        )).thenReturn(estimation);
    }

    public static void withLabelEstimation(StatisticsProvider stats, String label, long estimation) {
        when(stats.withLabel(
                argThat(l -> l != null && l.getLabel().equals(label))
        )).thenReturn(estimation);
    }

    public static void withPropertyEstimation(StatisticsProvider stats, String label, String property, long estimation) {
        when(stats.withProperty(
                argThat(l -> l != null && l.getLabel().equals(label)),
                argThat(p -> p != null && p.getKey().equals(property))
        )).thenReturn(estimation);
    }

    public static void withConnectivityEstimation(StatisticsProvider stats, String fromLabel, String toLabel, long estimation) {
        when(stats.connections(
                argThat(l -> l != null && l.getLabel().equals(fromLabel)),
                argThat(l -> l != null && l.getLabel().equals(toLabel))
        )).thenReturn(estimation);
    }
}