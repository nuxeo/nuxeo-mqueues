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

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Computation that count record.
 *
 * @since 9.1
 */
public class ComputationRecordCounter implements Computation {

    private final ComputationMetadata metadata;
    private final long intervalMs;
    private int count;

    /**
     * Output record counter every interval.
     */
    public ComputationRecordCounter(String name, Duration interval) {
        this.metadata = new ComputationMetadata(
                name,
                new HashSet<>(Arrays.asList("i1")),
                new HashSet<>(Arrays.asList("o1")));
        this.intervalMs = interval.toMillis();
    }

    @Override
    public void init(ComputationContext context) {
        context.setTimer("sum", System.currentTimeMillis() + intervalMs);
    }

    @Override
    public void destroy() {
    }

    @Override
    public void processRecord(ComputationContext context, String inputStreamName, Record record) {
        count += 1;
    }

    @Override
    public void processTimer(ComputationContext context, String key, long time) {
        context.produceRecord("o1", Integer.toString(count), null);
        count = 0;
        context.setCommit(true);
        context.setTimer("sum", System.currentTimeMillis() + intervalMs);
    }

    @Override
    public ComputationMetadata metadata() {
        return metadata;
    }
}
