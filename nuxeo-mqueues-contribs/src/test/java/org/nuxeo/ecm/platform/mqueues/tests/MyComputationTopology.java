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
package org.nuxeo.ecm.platform.mqueues.tests;

import org.nuxeo.ecm.platform.mqueues.Computations;
import org.nuxeo.lib.core.mqueues.computation.AbstractComputation;
import org.nuxeo.lib.core.mqueues.computation.ComputationContext;
import org.nuxeo.lib.core.mqueues.computation.Record;
import org.nuxeo.lib.core.mqueues.computation.Topology;

import java.util.Arrays;
import java.util.Map;

/**
 * @since 9.3
 */
public class MyComputationTopology implements Computations {

    @Override
    public Topology getTopology(Map<String, String> options) {
        return Topology.builder()
                .addComputation(() -> new ComputationNoop("C1"), Arrays.asList("i1:input", "o1:output"))
                .build();
    }

    // Simple computation that forward a record
    protected class ComputationNoop extends AbstractComputation {
        public ComputationNoop(String name) {
            super(name, 1, 1);
        }

        @Override
        public void processRecord(ComputationContext context, String inputStreamName, Record record) {
            System.out.println("GOT A RECORD: " + record);
            context.produceRecord("o1", record);
            context.askForCheckpoint();
        }
    }
}
