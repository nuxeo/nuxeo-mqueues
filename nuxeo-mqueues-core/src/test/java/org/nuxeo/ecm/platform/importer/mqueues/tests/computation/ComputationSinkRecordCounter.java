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

import org.nuxeo.ecm.platform.importer.mqueues.computation.Computation;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationContext;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationMetadata;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.w3c.dom.css.Counter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Computation that count record.
 * Produce a message with the total when receiving an input record
 *
 * @since 9.1
 */
public class ComputationSinkRecordCounter implements Computation {

    private final ComputationMetadata metadata;
    private int count;

    public ComputationSinkRecordCounter(String name) {
        this.metadata = new ComputationMetadata(
                name,
                new HashSet<>(Arrays.asList("i1", "i2")),
                new HashSet<>(Arrays.asList("o1")));
    }

    @Override
    public void init(ComputationContext context) {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void processRecord(ComputationContext context, String inputStreamName, Record record) {
        if ("i1".equals(inputStreamName)) {
            count += 1;
            if (count == 40) {
                System.out.println("XXXXXXXXX  WIN");
            }
        } else {
            context.produceRecord("o1", Integer.toString(count), null);
            count = 0;
        }
        context.setCommit(true);
    }

    @Override
    public void processTimer(ComputationContext context, String key, long time) {
    }

    @Override
    public ComputationMetadata metadata() {
        return metadata;
    }
}
