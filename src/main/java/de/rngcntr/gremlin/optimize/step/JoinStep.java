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

package de.rngcntr.gremlin.optimize.step;

import de.rngcntr.gremlin.optimize.query.JoinAttribute;
import de.rngcntr.gremlin.optimize.traverser.FakePathTraverser;
import de.rngcntr.gremlin.optimize.util.TraverserUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This step provides support for the join operator in Gremlin. This implementation only supports full outer equality
 * joins on labeled elments, which are produced by <code>select()</code> steps in Gremlin. The implemented join strategy
 * is a nested loops join.<br>
 * To apply a join on queries <code>a()</code> and <code>b()</code> the syntax is either <code>a().join(b())</code> or
 * <code>b().join(a())</code>.
 *
 * @author Florian Grieskamp
 */
public class JoinStep<E> extends FlatMapStep<E,Map<String,Object>> implements TraversalParent {

    private Iterator<Map<String,Object>> iterator = EmptyIterator.instance();

    private boolean initialized;
    private Traversal.Admin<Map<String,Object>, Map<String,Object>> matchTraversal;
    private final Set<JoinAttribute> joinAttributes;

    private List<Map<String,Object>> joinTuples;

    /**
     * Creates a {@link JoinStep} that joins the incoming traversers with the tuples returned by the inner traversal.
     *
     * @param traversal The parent traversal that this step belongs to.
     * @param matchTraversal The inner traversal that supplies the set of tuples to join with.
     */
    public JoinStep(Traversal.Admin<?,E> traversal, Traversal<E,?> matchTraversal, Set<JoinAttribute> joinAttributes) {
        super(traversal);
        this.initialized = false;
        this.matchTraversal = this.integrateChild(matchTraversal.asAdmin());
        this.joinAttributes = joinAttributes;
    }

    /**
     * Gets the inner traversal of the join.
     *
     * @return The inner traversal.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<Traversal.Admin<Map<String,Object>, Map<String,Object>>> getLocalChildren() {
        return Collections.singletonList(matchTraversal);
    }

    @Override
    protected Traverser.Admin<Map<String,Object>> processNextStart() {
        while (true) {
            if (this.iterator.hasNext()) {
                return new FakePathTraverser(this.iterator.next(), this.getNextStep(), 1L);
            } else {
                closeIterator();
                this.iterator = this.flatMap(this.starts.next());
            }
        }
    }

    /**
     * The first call initializes the step by executing the nested traversal and collecting it's result.
     * In addition, every call performs the inner loop of a nested loops join on the supplied traverser.
     *
     * @param traverser The input element that is checked against all join candidates from the inner traversal.
     * @return The joined mappings for the input traverser.
     */
    @Override
    protected Iterator<Map<String,Object>> flatMap(Traverser.Admin<E> traverser) {
        if (!initialized) {
            initialize();
        }

        return doNestedLoopsJoin(traverser);
    }

    /**
     * Executes the inner traversal and collects it's results.
     */
    private void initialize() {
        joinTuples = matchTraversal.toList();
        initialized = true;
    }

    /**
     * Performs the inner loop of a nested loops join on the supplied traverser.
     *
     * @param traverser The input element that is checked against all join candidates from the inner traversal.
     * @return The joined mappings for the input traverser.
     */
    private Iterator<Map<String,Object>> doNestedLoopsJoin(Traverser.Admin<E> traverser) {
        List<Map<String,Object>> results = new LinkedList<>();

        for (Map<String,Object> candidate : joinTuples) {
            if (match(TraverserUtils.mapHistory(traverser), candidate)) {
                for (int i = 0; i < traverser.bulk(); ++i) {
                    results.add(merge(candidate, TraverserUtils.mapHistory(traverser)));
                }
            }
        }

        return results.iterator();
    }

    /**
     * Checks whether two {@link Map}s match. The definition of a match is that both do not contain conflicting
     * information, i.e. different values for the same (join attribute) keys.
     *
     * @param a The first {@link Map}.
     * @param b The second {@link Map}.
     * @return <ul>
     *     <li><code>false</code> if a key exists in both {@link Map}s with different values</li>
     *     <li><code>true</code> otherwise.</li>
     * </ul>
     */
    private boolean match(Map<String,Object> a, Map<String,Object> b) {
        for (JoinAttribute attr : joinAttributes) {
            if (!attr.doMatch(a, b)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Merges two {@link Map}s into a single {@link HashMap} that contains all of the entries.
     *
     * @param inputs The input set of {@link Map}s to be merged.
     * @return The merged {@link HashMap}.
     */
    @SafeVarargs
    private final Map<String,Object> merge(Map<String, Object>... inputs) {
        Map<String,Object> result = new HashMap<>();
        for (Map<String, Object> input : inputs) {
            result.putAll(input);
        }
        return result;
    }

    /**
     * Creates a copy of this step that also contains a copy of the nested match traversal.
     *
     * @return The cloned join step.
     */
    @Override
    public JoinStep<E> clone() {
        final JoinStep<E> clone = (JoinStep<E>) super.clone();
        clone.matchTraversal = this.matchTraversal.clone();
        clone.matchTraversal.reset();
        return clone;
    }

    /**
     * Represents this step as a human readable text.
     *
     * @return A text representation of the join step.
     */
    public String toString() {
        return String.format("JoinStep({%s}, %s)",
                joinAttributes.stream().map(String::valueOf).collect(Collectors.joining(", ")), matchTraversal);
    }
}