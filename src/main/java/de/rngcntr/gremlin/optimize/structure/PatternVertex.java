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

package de.rngcntr.gremlin.optimize.structure;

import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentVertexRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectRetrieval;
import de.rngcntr.gremlin.optimize.retrieval.direct.DirectVertexRetrieval;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class PatternVertex extends PatternElement<Vertex> {
    private final Collection<PatternEdge> in;
    private final Collection<PatternEdge> out;

    public PatternVertex() {
        super(Vertex.class);
        in = new ArrayList<>();
        out = new ArrayList<>();
    }

    public void addEdge(PatternEdge edge, Direction direction) {
        switch (direction) {
            case IN:
                in.add(edge);
                break;
            case OUT:
                out.add(edge);
                break;
            default:
                throw new IllegalArgumentException("Not a valid direction: " + direction);
        }
    }

    @Override
    public DirectRetrieval<Vertex> generateDirectRetrieval() {
        return new DirectVertexRetrieval(this);
    }

    @Override
    public Collection<DependentRetrieval<Vertex>> generateDependentRetrievals() {
        ArrayList<DependentRetrieval<Vertex>> dependentRetrievals = new ArrayList<>();
        in.forEach(e -> dependentRetrievals.add(new DependentVertexRetrieval(this, e, Direction.IN)));
        out.forEach(e -> dependentRetrievals.add(new DependentVertexRetrieval(this, e, Direction.OUT)));
        return dependentRetrievals;
    }

    @Override
    public List<PatternElement<?>> getNeighbors() {
        ArrayList<PatternElement<?>> neighbors = new ArrayList<>(in.size() + out.size());
        neighbors.addAll(in);
        neighbors.addAll(out);
        return neighbors;
    }

    @Override
    public String toString() {
        String format = super.toString();
        String vertexSpecific = String.format("\n\tIn: %s\n\tOut: %s",
                in.stream().map(PatternElement::getId).collect(Collectors.toList()),
                out.stream().map(PatternElement::getId).collect(Collectors.toList()));
        return String.format(format, "VERTEX", vertexSpecific);
    }
}