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

package de.rngcntr.gremlin.optimize.statistics;

import org.apache.tinkerpop.gremlin.structure.Element;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.when;

public class MockedStatUtils {

    public static <E extends Element> void withTotalEstimation(StatisticsProvider stats, Class<E> clazz, long estimation) {
        when(stats.totals(
                argThat(c -> c != null && c.equals(clazz))
        )).thenReturn(estimation);
    }

    public static void withLabelEstimation(StatisticsProvider stats, String label, long estimation) {
        when(stats.withLabel(
                argThat(l -> l != null && l.getLabel().equals(label))
        )).thenReturn(estimation);
    }

    public static void withPropertyEstimation(StatisticsProvider stats, String label, String property, long estimation) {
        when(stats.withProperty(
                argThat(l -> l != null && l.getLabel().equals(label)),
                argThat(p -> p != null && p.getKey().equals(property))
        )).thenReturn(estimation);
    }

    public static void withConnectivityEstimation(StatisticsProvider stats, String fromLabel, String toLabel, long estimation) {
        when(stats.connections(
                argThat(l -> l != null && l.getLabel().equals(fromLabel)),
                argThat(l -> l != null && l.getLabel().equals(toLabel))
        )).thenReturn(estimation);
    }
}