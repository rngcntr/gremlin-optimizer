package de.rngcntr.gremlin.optimize.statistics;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.Map;

public class GraphStatistics implements StatisticsProvider {
    private static final GraphStatistics INSTANCE = new GraphStatistics();

    private long numV;
    private final Map<String, Long> numVLabel;

    private long numE;
    private final Map<String, Long> numELabel;

    private final Map<Pair<String, String>, Long> numVLabelToELabel;
    private final Map<Pair<String, String>, Long> numELabelToVLabel;

    public GraphStatistics() {
        numV = 0;
        numVLabel = new HashMap<>();

        numE = 0;
        numELabel = new HashMap<>();

        numVLabelToELabel = new HashMap<>();
        numELabelToVLabel = new HashMap<>();
    }

    public static GraphStatistics getInstance() {
        return INSTANCE;
    }

    public <E extends Element> long totals(Class<E> clazz) {
        if (Vertex.class.isAssignableFrom(clazz)) {
            return numV;
        } else if (Edge.class.isAssignableFrom(clazz)) {
            return numE;
        } else {
            return 0L;
        }
    }

    @Override
    public <E extends Element> long withLabel(LabelFilter<E> filter) {
        if (Vertex.class.isAssignableFrom(filter.getFilteredType())) {
            return numVLabel.getOrDefault(filter.getLabel(), 0L);
        } else if (Edge.class.isAssignableFrom(filter.getFilteredType())) {
            return numELabel.getOrDefault(filter.getLabel(), 0L);
        } else {
            return 0L;
        }
    }

    @Override
    public <E extends Element> long withProperty(PropertyFilter<E> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E1 extends Element, E2 extends Element> long connections(LabelFilter<E1> fromLabel, LabelFilter<E2> toLabel) {
        if (Vertex.class.isAssignableFrom(fromLabel.getFilteredType())) {
            if (Edge.class.isAssignableFrom(toLabel.getFilteredType())) {
                return numVLabelToELabel.getOrDefault(new Pair<>(fromLabel.getLabel(), toLabel.getLabel()), 0L);
            } else {
                return 0L;
            }
        } else if (Edge.class.isAssignableFrom(fromLabel.getFilteredType())) {
            if (Vertex.class.isAssignableFrom(toLabel.getFilteredType())) {
                return numELabelToVLabel.getOrDefault(new Pair<>(fromLabel.getLabel(), toLabel.getLabel()), 0L);
            } else {
                return 0L;
            }
        } else {
            return 0L;
        }
    }
}
