package de.rngcntr.gremlin.optimize.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class FakePathTraverser extends B_LP_O_S_SE_SL_Traverser<Map<String,Object>> {

    public FakePathTraverser(final Map<String,Object> t, final Step<Map<String,Object>, ?> step, final long initialBulk) {
        super(t, step, initialBulk);
        setPathFromMap(t);
        final Set<String> labels = step.getLabels();
        if (!labels.isEmpty()) this.path = this.path.extend(t, labels);
    }

    private void setPathFromMap(Map<String, Object> pathMap) {
        this.path = ImmutablePath.make();
        for(Map.Entry<String, Object> mapEntry : pathMap.entrySet()) {
            this.path = this.path.extend(mapEntry.getValue(), Collections.singleton(mapEntry.getKey()));
        }
    }

    public String toString() {
        return "FakeTraverser";
    }
}
