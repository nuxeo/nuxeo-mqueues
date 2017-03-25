/*
 * (C) Copyright 2017 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Taken from https://github.com/concord/concord-jvm
 */
package org.nuxeo.ecm.platform.importer.mqueues.computation;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ComputationMetadata {
    /** Globally unique identifier of the computation */
    public final String name;

    /** List of streams to subscribe this computation to. */
    public final Set<String> istreams;

    /** List of streams this computation may produce on */
    public final Set<String> ostreams;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComputationMetadata metadata = (ComputationMetadata) o;

        if (!name.equals(metadata.name)) return false;
        if (!istreams.equals(metadata.istreams)) return false;
        return ostreams.equals(metadata.ostreams);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + istreams.hashCode();
        result = 31 * result + ostreams.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ComputationMetadata{" +
                "name='" + name + '\'' +
                ", istreams=" + istreams.stream().collect(Collectors.joining(",")) +
                ", ostreams=" + ostreams.stream().collect(Collectors.joining(","))  +
                '}';
    }

    public ComputationMetadata(String name, Set<String> istreams, Set<String> ostreams) {
        this.name = Objects.requireNonNull(name);

        if (istreams == null) {
            this.istreams = Collections.emptySet();
        } else {
            this.istreams = istreams;
        }
        if (ostreams == null) {
            this.ostreams = Collections.emptySet();
        } else {
            this.ostreams = ostreams;
        }

        if (this.istreams.isEmpty() && this.ostreams.isEmpty()) {
            throw new RuntimeException("Both input and output streams are empty");
        }
    }
}
