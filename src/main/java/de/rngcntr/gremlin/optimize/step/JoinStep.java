package de.rngcntr.gremlin.optimize.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class JoinStep<E> extends FlatMapStep<HashMap<String,E>, HashMap<String,E>> {

    private boolean initialized;
    private final Traversal<?, HashMap<String,E>> matchTraversal;
    private final String[] joinAttributes;

    private List<HashMap<String,E>> joinTuples;

    public JoinStep(Traversal.Admin traversal, Traversal<?, HashMap<String,E>> matchTraversal, String... joinAttributes) {
        super(traversal);
        this.initialized = false;
        this.matchTraversal = matchTraversal;
        this.joinAttributes = joinAttributes;
    }

    @Override
    protected Iterator<HashMap<String,E>> flatMap(Traverser.Admin<HashMap<String,E>> traverser) {
        if (!initialized) {
            initialize();
        }

        return doNestedLoopsJoin(traverser);
    }

    private void initialize() {
        joinTuples = matchTraversal.toList();
        initialized = true;
    }

    private Iterator<HashMap<String,E>> doNestedLoopsJoin(Traverser.Admin<HashMap<String,E>> traverser) {
        List<HashMap<String,E>> results = new LinkedList<>();

        for (HashMap<String,E> probe : joinTuples) {
            if (match(probe, traverser.get())) {
                results.add(merge(probe, traverser.get()));
            }
        }

        return results.iterator();
    }

    private boolean match(HashMap<String,E> a, HashMap<String,E> b) {
        for (String attr : joinAttributes) {
            if (a.get(attr) != b.get(attr) || a.get(attr) == null) {
                return false;
            }
        }

        return true;
    }

    private HashMap<String,E> merge(HashMap<String,E> a, HashMap<String,E> b) {
        HashMap<String,E> result = new HashMap<>();
        result.putAll(a);
        result.putAll(b);
        return result;
    }
}
