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

package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A pattern element defines constraints that can later be used to match real elements from a real graph. Pattern
 * elements can be connected to form a {@link PatternGraph} which matches subgraphs of the data graph.
 *
 * @param <E> Either {@link Vertex} or {@link Edge}.
 * @author Florian Grieskamp
 */
public abstract class PatternElement<E extends Element> implements Comparable<PatternElement<?>> {
    private final Class<E> type;
    protected LabelFilter<E> labelFilter;
    protected Collection<PropertyFilter<E>> propertyFilters;
    protected List<Retrieval<E>> retrievals;
    protected long id;

    /**
     * Creates a new pattern element of the specified type with no label filter, no property filter and a generated
     * identifier.
     *
     * @param type Either {@link Vertex} or {@link Edge}.
     */
    public PatternElement(Class<E> type) {
        this.type = type;
        this.labelFilter = null;
        this.propertyFilters = new ArrayList<>();
        this.retrievals = new ArrayList<>();
        this.id = IdProvider.getInstance().getNextId();
    }

    /**
     * Gets the unique identifier of this element.
     *
     * @return The long identifier.
     */
    public long getId() {
        return id;
    }

    /**
     * Initializes all dependent and direct retrieval strategies possible for this element.
     */
    public void initializeRetrievals() {
        getRetrievals().add(generateDirectRetrieval());
        getRetrievals().addAll(generateDependentRetrievals());
    }

    /**
     * Estimates the result size of all direct retrievals by using the specified statistics provider.
     *
     * @param stats The statistics provider.
     */
    public void estimateDirectRetrievals(StatisticsProvider stats) {
        getRetrievals().stream()
                .filter(r -> r instanceof DirectRetrieval)
                .forEach(r -> r.estimate(stats));
    }

    /**
     * Estimates the result size of all dependent retrievals by using the specified statistics provider.
     *
     * @param stats The statistics provider.
     */
    public void estimateDependentRetrievals(StatisticsProvider stats) {
        getRetrievals().stream()
                .filter(r -> r instanceof DependentRetrieval)
                .forEach(r -> r.estimate(stats));
    }

    /**
     * Checks whether this element represents an edge.
     *
     * @return <ul>
     *     <li><code>true</code> if it is an edge</li>
     *     <li><code>false</code> otherwise</li>
     * </ul>
     */
    public boolean isEdge() {
        return type == Edge.class;
    }

    /**
     * Checks whether this element represents a vertex.
     *
     * @return <ul>
     *     <li><code>true</code> if it is a vertex</li>
     *     <li><code>false</code> otherwise</li>
     * </ul>
     */
    public boolean isVertex() {
        return type == Vertex.class;
    }

    /**
     * Sets the element's label filter to the specified filter.
     *
     * @param labelFilter The label filter to be used for this element.
     */
    public void setLabelFilter(LabelFilter<E> labelFilter) {
        this.labelFilter = labelFilter;
    }

    /**
     * Extends the set of property filters for this element by the specified filter.
     *
     * @param propertyFilter The additional property filter to be used for this element.
     */
    public void addPropertyFilter(PropertyFilter<E> propertyFilter) {
        this.propertyFilters.add(propertyFilter);
    }

    /**
     * Gets the cheapest retrieval strategy in terms of estimated result size of all available retrievals for this
     * element.
     *
     * @return The retrieval with the lowest estimated result size.
     * @throws NoSuchElementException If the no retrievals are initialized for this element.
     */
    public Retrieval<E> getBestRetrieval() throws NoSuchElementException {
        return Collections.min(getRetrievals());
    }

    /**
     * Creates the direct retrieval for this element.
     *
     * @return The direct retrieval.
     */
    protected abstract DirectRetrieval<E> generateDirectRetrieval();

    /**
     * Creates all possible dependent retrievals for this element.
     *
     * @return The dependent retrievals.
     */
    protected abstract Collection<DependentRetrieval<E>> generateDependentRetrievals();

    /**
     * Gets adjacent, incident or all neighboured elements to this element.
     *
     * @param direction The direction.
     * @return The list of neighbors.
     */
    public abstract List<PatternElement<?>> getNeighbors(Direction direction);

    /**
     * Checks whether this element has label filter specified.
     *
     * @return <ul>
     *     <li><code>true</code> if it has a label filter</li>
     *     <li><code>false</code> otherwise</li>
     * </ul>
     */
    public boolean hasLabelFilter() {
        return getLabelFilter() != null;
    }

    /**
     * Gets the label filter for this element.
     *
     * @return The label filter. <code>null</code> if no filter is specified.
     */
    public LabelFilter<E> getLabelFilter() {
        return labelFilter;
    }

    /**
     * Gets the property filters specified for this element.
     *
     * @return The collection of property filters.
     */
    public Collection<PropertyFilter<E>> getPropertyFilters() {
        return propertyFilters;
    }

    /**
     * Gets all the possible retrieval strategies for this element.
     *
     * @return The collection of retrievals.
     */
    public Collection<Retrieval<E>> getRetrievals() {
        return retrievals;
    }

    /**
     * Gets the type of this element.
     *
     * @return Either {@link Vertex} or {@link Edge}.
     */
    public Class<E> getType() {
        return type;
    }

    /**
     * Gets a human readable representation of this element.
     *
     * @return The text representation of the element.
     */
    @Override
    public String toString() {
        String labelString = getLabelFilter() == null || getLabelFilter().getLabel() == null
                ? ""
                : String.format(" (%s)", getLabelFilter());
        String retrievalString = getRetrievals().size() == 0
                ? "unknown"
                : getBestRetrieval().toString();
        return String.format("[%d] %%s%s\n\tProperties: %s%%s\n\tBest Retrieval: %s", getId(), labelString, getPropertyFilters(), retrievalString);
    }

    /**
     * Compares this element to another element by comparing their best estimated retrieval to each other.
     *
     * @param other The element to compare to.
     * @return <ul>
     *     <li>A positive integer if this element is expected to match more elements than the other element.</li>
     *     <li>0 if this element is expected to match exactly as many elements as the other element.</li>
     *     <li>A negative integer if this element is expected to match fewer elements than the other element.</li>
     * </ul>
     */
    @Override
    public int compareTo(PatternElement<?> other) {
        return getBestRetrieval().compareTo(other.getBestRetrieval());
    }

    /**
     * Gets all neighbors of this element for which their best retrieval strategy depends on this element being
     * retrieved in advance.
     *
     * @return The list of dependent neighbors.
     */
    public List<PatternElement<?>> getDependentNeighbors() {
        return getNeighbors(Direction.BOTH).stream()
            .map(PatternElement::getBestRetrieval)
            .filter(retrieval -> retrieval instanceof DependentRetrieval)
            .map(retrieval -> (DependentRetrieval<?>) retrieval)
            .filter(retrieval -> retrieval.getSource() == this)
            .map(DependentRetrieval::getElement).collect(Collectors.toList());
    }

    /**
     * Gets the best dependent retrieval for this element in terms of estimated size.
     *
     * @return The best dependent retrieval, wrapped in an instance of {@link Optional}.
     */
    public Optional<DependentRetrieval<E>> getBestDependentRetrieval() {
        return getRetrievals().stream()
                .filter(r -> r instanceof DependentRetrieval)
                .map(r -> (DependentRetrieval<E>) r)
                .min(Comparator.comparing(Retrieval::getEstimatedSize));
    }

    /**
     * Gets the best dependent retrieval for this element that depends on the given other element.
     *
     * @param sourceElement The element that the returned retrieval should depend on.
     * @return The dependent retrieval, wrapped in an instance of {@link Optional}.
     */
    public Optional<DependentRetrieval<E>> getDependentRetrieval(PatternElement<?> sourceElement) {
        return getRetrievals().stream()
                .filter(r -> r instanceof DependentRetrieval)
                .map(r -> (DependentRetrieval<E>) r)
                .filter(r -> r.getSource() == sourceElement)
                .min(Comparator.comparing(Retrieval::getEstimatedSize));
    }

    /**
     * Checks whether this element is isomorphic to an other pattern element with respect to the given element mapping.
     * Isomorphic in this case means that the set of incoming and outgoing neighbors for both elements are equal with
     * respect to the mapping.
     *
     * @param otherElement The element to compare with.
     * @param elementMapping Which element of the original graph should be mapped to which element of the compared
     *                       graph.
     * @return <ul>
     *     <li><code>true</code> if both elements are isomorphic.</li>
     *     <li><code>false</code> otherwise.</li>
     * </ul>
     */
    public boolean isIsomorphicTo(PatternElement<?> otherElement, Map<PatternElement<?>, PatternElement<?>> elementMapping) {
        Set<PatternElement<?>> expectedNeighborsIn = new HashSet<>();
        Set<PatternElement<?>> expectedNeighborsOut = new HashSet<>();
        getNeighbors(Direction.IN).forEach(n -> expectedNeighborsIn.add(elementMapping.get(n)));
        getNeighbors(Direction.OUT).forEach(n -> expectedNeighborsOut.add(elementMapping.get(n)));
        return expectedNeighborsIn.equals(new HashSet<>(otherElement.getNeighbors(Direction.IN))) &&
                expectedNeighborsOut.equals(new HashSet<>(otherElement.getNeighbors(Direction.OUT)));
    }

    /**
     * Compares this element to another object. Criteria for equality are:
     * <ul>
     *     <li>The other object must be an instance of {@link PatternElement}</li>
     *     <li>Both need to share the same:
     *     <ul>
     *         <li>type</li>
     *         <li>label filter</li>
     *         <li>property filters</li>
     *     </ul>
     * </ul>
     *
     * @param other The other object.
     * @return <ul>
     *     <li><code>true</code> if both are equal.</li>
     *     <li><code>false</code> otherwise.</li>
     * </ul>
     */
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PatternElement)) return false;
        PatternElement<?> otherElement = (PatternElement<?>) other;
        if (this.getType() != otherElement.getType()) return false;
        if (!this.hasLabelFilter() && ((PatternElement<?>) other).hasLabelFilter()) return false;
        if (this.hasLabelFilter() && !this.getLabelFilter().equals(otherElement.getLabelFilter())) return false;
        if (getPropertyFilters().size() != otherElement.getPropertyFilters().size()) return false;
        return getPropertyFilters().containsAll(otherElement.getPropertyFilters());
    }
}