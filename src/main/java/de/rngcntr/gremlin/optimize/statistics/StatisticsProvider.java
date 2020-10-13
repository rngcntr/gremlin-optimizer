package de.rngcntr.gremlin.optimize.statistics;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * Graph databases that implement StatisticsProvider can be used to optimize Gremlin queries depending on the
 * distribution of labels and properties on vertices and edges.
 * An instance of StatisticsProvider is necessary to call the
 * {@link de.rngcntr.gremlin.optimize.structure.PatternGraph PatternGraph}'s
 * {@link de.rngcntr.gremlin.optimize.structure.PatternGraph#optimize optimize} method.
 */
public interface StatisticsProvider {
    /**
     * Returns the total amount (or an estimation) of the specified element in the entire graph.
     *
     * @param clazz The type of element to be searched for.
     * @param <E> Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     * @return The number of matching elements.
     */
    <E extends Element> long totals(Class<E> clazz);

    /**
     * Returns the total amount (or an estimation) of the specified element having a given label in the entire graph.
     *
     * @param label The specification of the elements label.
     * @param <E> Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     * @return The number of matching elements.
     */
    <E extends Element> long withLabel(LabelFilter<E> label);

    /**
     * Returns the total amount (or an estimation) of the specified element having a given property in the entire graph.
     *
     * @param property The specification of the element's label.
     * @param property The specification of the element's property.
     * @param <E> Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     * @return The number of matching elements.
     */
    <E extends Element> long withProperty(LabelFilter<E> label, PropertyFilter<E> property);

    /**
     * Returns the total amount (or an estimation) of the number of connections between two elements with the given
     * label restrictions. Valid connections with respect to the direction of edges include:
     * <ul>
     *     <li>Vertex to Vertex</li>
     *     <li>Vertex to Edge</li>
     *     <li>Edge to Vertex</li>
     * </ul>
     *
     * @param fromLabel The specification of the incoming element's label.
     * @param toLabel The specification of the outgoing element's label.
     * @param <E1> Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     * @param <E2> Either {@link org.apache.tinkerpop.gremlin.structure.Vertex} or
     *            {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     * @return The number of matching connections.
     */
    <E1 extends Element, E2 extends Element> long connections(LabelFilter<E1> fromLabel, LabelFilter<E2> toLabel);
}
