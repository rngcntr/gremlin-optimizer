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

package de.rngcntr.gremlin.optimize.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Heavily inspired by <a href="https://gist.github.com/jaycobbcruz/cbabc1eb49f51bfe2ed1db10a06a2b26">jaycobbcruz</a>.
 */
public class Permutations {

    public static <T> Stream<List<T>> of(final List<T> items) {
        return LongStream.range(0, factorial(items.size()))
                .mapToObj(i -> permutation(i, new ArrayList<>(items), new ArrayList<>()));
    }

    private static long factorial(final long n) {
        return LongStream.rangeClosed(2, n).reduce(1, (long x, long y) -> x * y);
    }

    private static <T> List<T> permutation(final long i, final List<T> input, final List<T> output) {
        if (input.isEmpty()) { return output; }

        final long factorial = factorial(input.size() - 1);
        output.add(input.remove((int) (i / factorial)));
        return permutation(i % factorial, input, output);
    }
}