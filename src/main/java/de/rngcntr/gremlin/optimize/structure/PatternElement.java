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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;

public abstract class PatternElement<E extends Element> implements Comparable<PatternElement<?>> {
    private final Class<E> type;
    protected LabelFilter<E> labelFilter;
    protected Collection<PropertyFilter<E>> propertyFilters;
    protected List<Retrieval<E>> retrievals;
    protected long id;

    public PatternElement(Class<E> type) {
        this.type = type;
        this.labelFilter = null;
        this.propertyFilters = new ArrayList<>();
        this.retrievals = new ArrayList<>();
        this.id = IdProvider.getInstance().getNextId();
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof PatternElement)) return false;
        PatternElement<?> otherElement = (PatternElement<?>) other;
        if (this.type != otherElement.type) return false;
        if (this.labelFilter == null && otherElement.labelFilter != null) return false;
        if (this.labelFilter != null && !this.labelFilter.equals(otherElement.labelFilter)) return false;
        if (propertyFilters.size() != otherElement.propertyFilters.size()) return false;
        return propertyFilters.containsAll(otherElement.propertyFilters);
    }

    public long getId() {
        return id;
    }

    public void initializeRetrievals() {
        retrievals.add(generateDirectRetrieval());
        retrievals.addAll(generateDependentRetrievals());
    }

    public void estimateDirectRetrievals(StatisticsProvider stats) {
        retrievals.stream()
                .filter(r -> r instanceof DirectRetrieval)
                .forEach(r -> r.estimate(stats));
    }

    public void estimateDependentRetrievals(StatisticsProvider stats) {
        retrievals.stream()
                .filter(r -> r instanceof DependentRetrieval)
                .forEach(r -> r.estimate(stats));
    }

    public boolean isEdge() {
        return type == Edge.class;
    }

    public boolean isVertex() {
        return type == Vertex.class;
    }

    public void setLabelFilter(LabelFilter<E> labelFilter) {
        this.labelFilter = labelFilter;
    }

    public void addPropertyFilter(PropertyFilter<E> propertyFilter) {
        this.propertyFilters.add(propertyFilter);
    }

    public Retrieval<E> getBestRetrieval() {
        return Collections.min(retrievals);
    }

    public abstract DirectRetrieval<E> generateDirectRetrieval();

    public abstract Collection<DependentRetrieval<E>> generateDependentRetrievals();

    public abstract List<PatternElement<?>> getNeighbors();

    public boolean hasLabelFilter() {
        return labelFilter != null;
    }

    public LabelFilter<E> getLabelFilter() {
        return labelFilter;
    }

    public Collection<PropertyFilter<E>> getPropertyFilters() {
        return propertyFilters;
    }

    public Class<E> getType() {
        return type;
    }

    @Override
    public String toString() {
        String labelString = labelFilter == null || labelFilter.getLabel() == null
                ? ""
                : String.format(" (%s)", labelFilter);
        String retrievalString = retrievals.size() == 0
                ? "unknown"
                : getBestRetrieval().toString();
        return String.format("[%d] %%s%s\n\tProperties: %s%%s\n\tBest Retrieval: %s", id, labelString, propertyFilters, retrievalString);
    }

    @Override
    public int compareTo(PatternElement<?> other) {
        return getBestRetrieval().compareTo(other.getBestRetrieval());
    }

    public List<PatternElement<?>> getDependentNeighbors() {
        return getNeighbors().stream()
            .map(PatternElement::getBestRetrieval)
            .filter(retrieval -> retrieval instanceof DependentRetrieval)
            .map(retrieval -> (DependentRetrieval<?>) retrieval)
            .filter(retrieval -> retrieval.getSource() == this)
            .map(DependentRetrieval::getElement).collect(Collectors.toList());
    }

    public Optional<DependentRetrieval<E>> getBestDependentRetrieval() {
        return retrievals.stream()
                .filter(r -> r instanceof DependentRetrieval)
                .map(r -> (DependentRetrieval<E>) r)
                .min(Comparator.comparing(Retrieval::getEstimatedSize));
    }

    public Optional<DependentRetrieval<E>> getDependentRetrieval(PatternElement<?> dependentElement) {
        return retrievals.stream()
                .filter(r -> r instanceof DependentRetrieval)
                .map(r -> (DependentRetrieval<E>) r)
                .filter(r -> r.getSource() == dependentElement)
                .min(Comparator.comparing(Retrieval::getEstimatedSize));
    }

    public boolean isIsomorphicTo(PatternElement<?> otherElement, Map<PatternElement<?>, PatternElement<?>> elementMapping) {
        Set<PatternElement<?>> expectedNeighbors = new HashSet<>();
        getNeighbors().forEach(n -> expectedNeighbors.add(elementMapping.get(n)));
        return expectedNeighbors.equals(new HashSet<>(otherElement.getNeighbors()));
    }
}