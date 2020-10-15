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

package de.rngcntr.gremlin.optimize.testutils.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.HashSet;
import java.util.Set;

public class UniqueEdgeCountStep extends UniqueElementCountStep<Edge> {
    private static Set<Edge> seenEdges = new HashSet<>();

    public UniqueEdgeCountStep(Traversal.Admin<?, ?> traversal) {
        super(traversal);
    }

    @Override
    void registerElement(Edge edge) {
        seenEdges.add(edge);
    }

    public static int getCount() {
        return seenEdges.size();
    }

    public static void resetCount() {
        seenEdges = new HashSet<>();
    }
}
