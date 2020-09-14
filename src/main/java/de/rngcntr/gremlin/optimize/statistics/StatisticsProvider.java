package de.rngcntr.gremlin.optimize.statistics;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import org.apache.tinkerpop.gremlin.structure.Element;

public interface StatisticsProvider {
    <E extends Element> long totals(Class<E> clazz);
    <E extends Element> long withLabel(LabelFilter<E> label);
    <E extends Element> long withProperty(PropertyFilter<E> property);
    <E1 extends Element, E2 extends Element> long connections(LabelFilter<E1> fromLabel, LabelFilter<E2> toLabel);
}
