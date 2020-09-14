package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.Retrieval;
import de.rngcntr.gremlin.optimize.statistics.StatisticsProvider;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.*;

public abstract class PatternElement<E extends Element> implements Comparable<PatternElement<?>> {
    private final Class<E> type;
    protected boolean isFinal;
    protected LabelFilter<E> labelFilter;
    protected Collection<PropertyFilter<E>> propertyFilters;
    protected List<Retrieval<E>> retrievals;
    protected String stepLabel;
    protected int id;

    public PatternElement(Class<E> type) {
        this.type = type;
        this.isFinal = false;
        this.labelFilter = null;
        this.propertyFilters = new ArrayList<>();
        this.retrievals = new ArrayList<>();
        this.stepLabel = null;
        this.id = IdProvider.getInstance().getNextId();
    }

    public void setStepLabel(String stepLabel) {
        this.stepLabel = stepLabel;
    }

    public int getId() {
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
    public int compareTo(PatternElement<?> other) {
        return getBestRetrieval().compareTo(other.getBestRetrieval());
    }
}
