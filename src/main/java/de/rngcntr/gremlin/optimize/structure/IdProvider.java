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

/**
 * Used to generate increasing longs to use as identifiers for pattern elements. The long IDs could also be
 * replaced by UUIDs but that would impact readability in a negative way.
 *
 * @author Florian Grieskamp
 */
public class IdProvider {
    private static long nextId = 0L;
    private static final IdProvider INSTANCE = new IdProvider();

    /**
     * Gets the singleton instance of {@link IdProvider}.
     *
     * @return The instance.
     */
    public static IdProvider getInstance() {
        return INSTANCE;
    }

    /**
     * Gets the next unique long identifier.
     *
     * @return The next ID.
     */
    public synchronized long getNextId() {
        return nextId++;
    }
}