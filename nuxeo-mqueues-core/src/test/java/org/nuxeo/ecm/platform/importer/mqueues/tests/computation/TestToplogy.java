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
 *     bdelbosc
 */
package org.nuxeo.ecm.platform.importer.mqueues.tests.computation;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Topology;

import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @since 9.1
 */
public class TestToplogy {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Test
    public void testTopology() throws Exception {

        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("C1"), Arrays.asList("i1:input", "o1:s1"))
                .addComputation(() -> new ComputationForward("C2", 1, 2), Arrays.asList("i1:s1", "o1:s2", "o2:s3"))
                .addComputation(() -> new ComputationForward("C3", 2, 1), Arrays.asList("i1:s1", "i2:s4", "o1:output"))
                .addComputation(() -> new ComputationForward("C4", 1, 2), Arrays.asList("i1:s2", "o1:output", "o2:s4"))
                .addComputation(() -> new ComputationForward("C5", 1, 1), Arrays.asList("i1:s3", "o1:output"))
                .build();

        assertNotNull(topology);
        assertEquals(6, topology.streamsSet().size());
        assertEquals(5, topology.metadataList().size());

        assertEquals(new HashSet<>(), topology.getAncestorComputationNames("C1"));
        assertEquals(new HashSet<>(Arrays.asList("C1")), topology.getAncestorComputationNames("C2"));
        assertEquals(new HashSet<>(Arrays.asList("C1", "C2", "C4")), topology.getAncestorComputationNames("C3"));
        assertEquals(new HashSet<>(Arrays.asList("C1", "C2")), topology.getAncestorComputationNames("C4"));
        assertEquals(new HashSet<>(Arrays.asList("C1", "C2")), topology.getAncestorComputationNames("C5"));

        // there is no sink or source in this dag because we have external streams "input" and "output"
        assertFalse(topology.isSource("C1"));
        assertFalse(topology.isSink("C5"));

        assertEquals(new HashSet<>(), topology.getParents("input"));
        assertEquals(new HashSet<>(Arrays.asList("input")), topology.getParents("C1"));
        assertEquals(new HashSet<>(Arrays.asList("s1")), topology.getChildren("C1"));

        assertEquals(new HashSet<>(Arrays.asList("s2", "s3")), topology.getChildren("C2"));
        assertEquals(new HashSet<>(Arrays.asList("s1")), topology.getParents("C2"));

        assertEquals(new HashSet<>(Arrays.asList("C3", "C4", "C5")), topology.getParents("output"));
        assertEquals(new HashSet<>(Arrays.asList("C1", "C2", "C3", "C4", "C5", "input", "s1", "s2", "s3", "s4")),
                topology.getAncestors("output"));
        assertEquals(new HashSet<>(Arrays.asList("C1", "C2", "C3", "C4", "C5", "s1", "s2", "s3", "s4", "output")),
                topology.getDescendants("input"));

        // check plantuml representation
        assertTrue(topology.toPlantuml().startsWith("@startuml"));
        assertTrue(topology.toPlantuml().endsWith("@enduml\n"));

    }


}
