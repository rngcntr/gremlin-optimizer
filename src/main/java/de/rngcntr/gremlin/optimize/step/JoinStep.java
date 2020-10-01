package de.rngcntr.gremlin.optimize.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;

import java.util.*;

public class JoinStep extends FlatMapStep<Map<String,Object>, Map<String,Object>> implements TraversalParent {

    private boolean initialized;
    private final Traversal.Admin<?,?> traversal;
    private final Traversal<?, Map<String,Object>> matchTraversal;

    private List<Map<String,Object>> joinTuples;

    public JoinStep(Traversal.Admin<?,?> traversal, Traversal<?, Map<String,Object>> matchTraversal) {
        super(traversal);
        this.initialized = false;
        this.traversal = traversal;
        this.matchTraversal = matchTraversal;
        this.integrateChild(matchTraversal.asAdmin());
    }

    @Override
    protected Iterator<Map<String,Object>> flatMap(Traverser.Admin<Map<String,Object>> traverser) {
        if (!initialized) {
            initialize();
        }

        return doNestedLoopsJoin(traverser);
    }

    private void initialize() {
        assert traversal.getGraph().isPresent();
        matchTraversal.asAdmin().setGraph(traversal.getGraph().get());
        joinTuples = matchTraversal.toList();
        initialized = true;
    }

    private Iterator<Map<String,Object>> doNestedLoopsJoin(Traverser.Admin<Map<String,Object>> traverser) {
        List<Map<String,Object>> results = new LinkedList<>();

        for (Map<String,Object> probe : joinTuples) {
            if (match(probe, traverser.get())) {
                results.add(merge(probe, traverser.get()));
            }
        }

        return results.iterator();
    }

    private boolean match(Map<String,Object> a, Map<String,Object> b) {
        for (String attr : a.keySet()) {
            if (b.containsKey(attr) && a.get(attr) != b.get(attr)) {
                return false;
            }
        }

        return true;
    }

    private Map<String,Object> merge(Map<String,Object> a, Map<String,Object> b) {
        Map<String,Object> result = new HashMap<>();
        result.putAll(a);
        result.putAll(b);
        return result;
    }

    public String toString() {
        return String.format("JoinStep(%s)", matchTraversal);
    }
}
