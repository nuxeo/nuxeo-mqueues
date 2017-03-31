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
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Computation that procduce records.
 *
 * @since 9.1
 */
public class ComputationSource implements Computation {

    private final ComputationMetadata metadata;
    private final int records;
    private final int batchSize;
    private int generated = 0;
    private long targetTimestamp;

    public ComputationSource(String name) {
        this(name, 1, 10, 3, 0);
    }

    /**
     * The targetTimestamp will be used for the last batch of record
     */
    public ComputationSource(String name, int outputs, int records, int batchSize, long targetTimestamp) {
        if (outputs <= 0) {
            throw new IllegalArgumentException("Can produce records without outputs");
        }
        this.records = records;
        this.batchSize = batchSize;
        this.targetTimestamp = targetTimestamp;
        this.metadata = new ComputationMetadata(
                name,
                Collections.emptySet(),
                IntStream.range(1, outputs + 1).boxed().map(i -> "o" + i).collect(Collectors.toSet()));
    }

    @Override
    public void init(ComputationContext context) {
        context.setTimer("generate", System.currentTimeMillis());
    }

    @Override
    public void destroy() {
    }

    @Override
    public void processRecord(ComputationContext context, String inputStreamName, Record record) {
    }

    @Override
    public void processTimer(ComputationContext context, String key, long time) {
        if ("generate".equals(key)) {
            long lastWatermark = 0;
            for (int i = 0; (i < batchSize) && (generated < records); i++) {
                Record record = getRandomRecord(++generated);
                lastWatermark = record.watermark = getWatermark();
                metadata.ostreams.forEach(o -> context.produceRecord(o, record));
                if (generated % 100 == 0) {
                    System.out.println("Generate record: " + generated + " wm " + lastWatermark);
                }
            }
            context.setCommit(true);
            if (generated < records) {
                context.setTimer("generate", System.currentTimeMillis());
            } else {
                // set computation low watermark to the target computation;
                context.setSourceLowWatermark(Watermark.completedOf(Watermark.ofTimestamp(targetTimestamp)).getValue());
            }
        }
    }

    private long getWatermark() {
        return Watermark.ofTimestamp(targetTimestamp - (records - generated)).getValue();
    }

    @Override
    public ComputationMetadata metadata() {
        return metadata;
    }

    public Record getRandomRecord(int i) {
        String msg = "data from " + metadata.name + " msg " + i;
        return Record.of("key" + i, msg.getBytes());
    }
}
