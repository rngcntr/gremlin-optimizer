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
    protected boolean isFinal;
    protected LabelFilter<E> labelFilter;
    protected Collection<PropertyFilter<E>> propertyFilters;
    protected List<Retrieval<E>> retrievals;
    protected long id;

    public PatternElement(Class<E> type) {
        this.type = type;
        this.isFinal = false;
        this.labelFilter = null;
        this.propertyFilters = new ArrayList<>();
        this.retrievals = new ArrayList<>();
        this.id = IdProvider.getInstance().getNextId();
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

    public void makeFinal() {
        this.isFinal = true;
    }

    public boolean isFinal() {
        return isFinal;
    }

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
                ? "" : String.format(" (%s)", labelFilter);
        return String.format("[%d] %%s%s\n\tProperties: %s%%s", id, labelString, propertyFilters);
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
                .filter(r -> r.getSource().equals(dependentElement))
                .min(Comparator.comparing(Retrieval::getEstimatedSize));
    }
}
