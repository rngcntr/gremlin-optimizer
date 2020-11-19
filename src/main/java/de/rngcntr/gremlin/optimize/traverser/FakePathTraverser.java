package de.rngcntr.gremlin.optimize.traverser;

import de.rngcntr.gremlin.optimize.util.TraverserUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class FakePathTraverser<E> extends B_LP_O_S_SE_SL_Traverser<E> {

    public FakePathTraverser(final E location, final Map<String,Object> path, final Step<E, ?> step, final long initialBulk) {
        super(location, step, initialBulk);
        setPathFromMap(path);
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
        return String.format("(location=%s, path=%s)", t, TraverserUtils.mapHistory(this));
    }
}
