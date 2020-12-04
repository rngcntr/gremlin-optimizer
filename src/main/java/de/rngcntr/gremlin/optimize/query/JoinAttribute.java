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

import de.rngcntr.gremlin.optimize.retrieval.dependent.DependentRetrieval;
import de.rngcntr.gremlin.optimize.structure.PatternElement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JoinAttribute {

    private PatternElement<?> leftElement, rightElement;
    private MatchOn leftMatch, rightMatch;

    public enum MatchOn {
        ELEMENT, IN, OUT;
    }

    public enum JoinPosition {
        LEFT, RIGHT;
    }

    public JoinAttribute(PatternElement<?> leftElement, MatchOn leftMatch, PatternElement<?> rightElement, MatchOn rightMatch) {
        this.leftElement = leftElement;
        this.rightElement = rightElement;
        this.leftMatch = leftMatch;
        this.rightMatch = rightMatch;
    }

    public JoinAttribute(PatternElement<?> bothElements) {
        this(bothElements, MatchOn.ELEMENT, bothElements, MatchOn.ELEMENT);
    }

    public Set<PatternElement<?>> getElements() {
        Set<PatternElement<?>> s = new HashSet();
        s.add(leftElement);
        s.add(rightElement);
        return s;
    }

    public String toString() {
        return String.format("%s.%s=%s.%s", leftElement.getId(), leftMatch, rightElement.getId(), rightMatch);
    }

    public boolean doMatch(Map<String, Object> left, Map<String, Object> right) {
        Object leftCandidate = left.get(String.valueOf(leftElement.getId()));
        Object rightCandidate = right.get(String.valueOf(rightElement.getId()));

        if (leftCandidate == null || rightCandidate == null) {
            return true;
        }

        return resolve(leftCandidate, leftMatch) == resolve(rightCandidate, rightMatch);
    }

    public void reformat(DependentRetrieval<?> reorderedRetrieval, JoinPosition pos) {
        final PatternElement<?> oldElement = reorderedRetrieval.getElement();
        final PatternElement<?> newElement = reorderedRetrieval.getSource();
        MatchOn newMatch = reorderedRetrieval.getDirection() == Direction.IN ? MatchOn.IN : MatchOn.OUT;
        switch(pos) {
            case LEFT:
                if (leftElement == oldElement) {
                    leftElement = newElement;
                    leftMatch = newMatch;
                }
                break;
            case RIGHT:
                if (rightElement == oldElement) {
                    rightElement = newElement;
                    rightMatch = newMatch;
                }
                break;
        }
    }

    private Object resolve(Object candidate, MatchOn matchOn) {
        if (matchOn == MatchOn.ELEMENT) {
            return candidate;
        }

        if (!(candidate instanceof Edge)) {
            throw new IllegalArgumentException(
                    String.format("%s can only be used on Edges but was used on %s", matchOn, candidate));
        }

        Edge e = (Edge) candidate;
        switch (matchOn) {
            case IN:
                return e.inVertex();
            case OUT:
                return e.outVertex();
            default:
                throw new IllegalArgumentException(String.format("Unhandled match type: %s", matchOn));
        }
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof JoinAttribute)) return false;
        JoinAttribute otherAttribute = (JoinAttribute) other;
        return leftElement == otherAttribute.leftElement && rightElement == otherAttribute.rightElement
                && leftMatch == otherAttribute.leftMatch && rightMatch == otherAttribute.rightMatch;
    }
}
