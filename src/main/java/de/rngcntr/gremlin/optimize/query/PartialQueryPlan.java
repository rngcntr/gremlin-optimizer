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

package de.rngcntr.gremlin.optimize.query;

import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Set;

public interface PartialQueryPlan {
    Set<PatternElement<?>> getElements();
    GraphTraversal<Object, Object> asTraversal();
    Set<PartialQueryPlan> generalCut(Set<PatternElement<?>> elementsToKeep);
    Set<DependencyTree> explicitCut(Set<PatternElement<?>> elementsToKeep);
    boolean isMovable();
}
