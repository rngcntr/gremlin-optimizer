package de.rngcntr.gremlin.optimize.util;

import de.rngcntr.gremlin.optimize.filter.LabelFilter;
import de.rngcntr.gremlin.optimize.filter.PropertyFilter;
import de.rngcntr.gremlin.optimize.structure.PatternEdge;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import de.rngcntr.gremlin.optimize.structure.PatternVertex;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.*;

public class GremlinParser {
    public static PatternElement<?> parseGraphStep(GraphStep<?,?> graphStep) {
        if (graphStep.returnsVertex()) {
            return new PatternVertex();
        } else {
            return new PatternEdge();
        }
    }

    public static <E extends Element> void parseHasStep(HasStep<?> hasStep, PatternElement<E> currentElement) {
        for (HasContainer hc : hasStep.getHasContainers()) {
            if (hc.getKey().equals(T.label.getAccessor())) {
                // assuming P.eq(label)
                LabelFilter<E> filter = new LabelFilter<>(currentElement.getType(), (String) hc.getValue());
                currentElement.setLabelFilter(filter);
            } else {
                PropertyFilter<E> filter = new PropertyFilter<>(currentElement.getType(), hc.getKey(), hc.getPredicate());
                currentElement.addPropertyFilter(filter);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static List<PatternElement<?>> parseVertexStep(VertexStep<?> vertexStep, PatternVertex currentElement) {
        if (vertexStep.returnsVertex()) {
            return parseVertexVertexStep((VertexStep<Vertex>) vertexStep, currentElement);
        } else {
            return parseVertexEdgeStep((VertexStep<Edge>) vertexStep, currentElement);
        }
    }

    public static List<PatternElement<?>> parseVertexVertexStep(VertexStep<Vertex> vertexStep, PatternVertex currentVertex) {
        String edgeLabel = vertexStep.getEdgeLabels().length > 0 ? vertexStep.getEdgeLabels()[0] : null;
        Direction direction = vertexStep.getDirection();

        PatternEdge newEdge = createEdge(edgeLabel, currentVertex, direction);

        PatternVertex newVertex = new PatternVertex();
        newEdge.setVertex(newVertex, direction.opposite());
        newVertex.addEdge(newEdge, vertexStep.getDirection().opposite());

        return Arrays.asList(newVertex, newEdge);
    }

    public static List<PatternElement<?>> parseVertexEdgeStep(VertexStep<Edge> vertexStep, PatternVertex currentVertex) {
        String edgeLabel = vertexStep.getEdgeLabels().length > 0 ? vertexStep.getEdgeLabels()[0] : null;
        return Collections.singletonList(createEdge(edgeLabel, currentVertex, vertexStep.getDirection()));
    }

    public static Set<String> parseSelectStep(Scoping selectStep) {
        return selectStep.getScopeKeys();
    }

    public static PatternEdge createEdge(String label, PatternVertex neighbor, Direction direction) {
        PatternEdge newEdge = new PatternEdge();

        if (label != null) {
            LabelFilter<Edge> edgeLabelFilter = new LabelFilter<>(Edge.class, label);
            newEdge.setLabelFilter(edgeLabelFilter);
        }

        neighbor.addEdge(newEdge, direction);
        newEdge.setVertex(neighbor, direction);

        return newEdge;
    }

    public static PatternVertex parseEdgeStep(EdgeVertexStep edgeStep, PatternEdge currentEdge) {
        Direction direction = edgeStep.getDirection();
        PatternVertex newVertex = new PatternVertex();

        currentEdge.setVertex(newVertex, direction);
        newVertex.addEdge(currentEdge, direction);

        return newVertex;
    }

}
