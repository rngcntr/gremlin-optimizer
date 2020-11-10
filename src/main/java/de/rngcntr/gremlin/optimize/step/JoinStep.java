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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;

import java.util.*;

/**
 * This step provides support for the join operator in Gremlin. This implementation only supports full outer equality
 * joins on labeled elments, which are produced by <code>select()</code> steps in Gremlin. The implemented join strategy
 * is a nested loops join.<br>
 * To apply a join on queries <code>a()</code> and <code>b()</code> the syntax is either <code>a().join(b())</code> or
 * <code>b().join(a())</code>.
 *
 * @author Florian Grieskamp
 */
public class JoinStep extends FlatMapStep<Map<String,Object>, Map<String,Object>> implements TraversalParent {

    private boolean initialized;
    private Traversal.Admin<Map<String,Object>, Map<String,Object>> matchTraversal;
    private final Set<String> joinAttributes;

    private List<Map<String,Object>> joinTuples;

    /**
     * Creates a {@link JoinStep} that joins the incoming traversers with the tuples returned by the inner traversal.
     *
     * @param traversal The parent traversal that this step belongs to.
     * @param matchTraversal The inner traversal that supplies the set of tuples to join with.
     */
    public JoinStep(Traversal.Admin<?,?> traversal, Traversal<Map<String,Object>, Map<String,Object>> matchTraversal, Set<String> joinAttributes) {
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

    /**
     * The first call initializes the step by executing the nested traversal and collecting it's result.
     * In addition, every call performs the inner loop of a nested loops join on the supplied traverser.
     *
     * @param traverser The input element that is checked against all join candidates from the inner traversal.
     * @return The joined mappings for the input traverser.
     */
    @Override
    protected Iterator<Map<String,Object>> flatMap(Traverser.Admin<Map<String,Object>> traverser) {
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
    private Iterator<Map<String,Object>> doNestedLoopsJoin(Traverser.Admin<Map<String,Object>> traverser) {
        List<Map<String,Object>> results = new LinkedList<>();

        for (Map<String,Object> candidate : joinTuples) {
            if (match(candidate, traverser.get())) {
                results.add(merge(candidate, traverser.get()));
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
        for (String attr : joinAttributes) {
            if (a.get(attr) != b.get(attr)) {
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
    public JoinStep clone() {
        final JoinStep clone = (JoinStep) super.clone();
        clone.matchTraversal = this.matchTraversal.clone();
        return clone;
    }

    /**
     * Represents this step as a human readable text.
     *
     * @return A text representation of the join step.
     */
    public String toString() {
        return String.format("JoinStep({%s}, %s)", String.join(", ", joinAttributes), matchTraversal);
    }
}