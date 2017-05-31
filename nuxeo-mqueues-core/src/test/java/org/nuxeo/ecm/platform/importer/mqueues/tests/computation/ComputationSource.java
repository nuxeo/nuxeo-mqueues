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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.computation.AbstractComputation;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationContext;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;

/**
 * Source computation that produces random records.
 *
 * Source computation must take care of submitting ordered watermark
 * The target timestamp is used to build the watermark of the last record.
 *
 * @since 9.1
 */
public class ComputationSource extends AbstractComputation {
    private static final Log log = LogFactory.getLog(ComputationSource.class);
    private final int records;
    private final int batchSize;
    private int generated = 0;
    private long targetTimestamp;

    public ComputationSource(String name) {
        this(name, 1, 10, 3, 0);
    }

    public ComputationSource(String name, int outputs, int records, int batchSize, long targetTimestamp) {
        super(name, 0, outputs);
        if (outputs <= 0) {
            throw new IllegalArgumentException("Can produce records without output streams");
        }
        this.records = records;
        this.batchSize = batchSize;
        this.targetTimestamp = targetTimestamp;
    }

    @Override
    public void init(ComputationContext context) {
        context.setTimer("generate", System.currentTimeMillis());
    }

    @Override
    public void processRecord(ComputationContext context, String inputStreamName, Record record) {
        // source computation has no input
    }

    @Override
    public void processTimer(ComputationContext context, String key, long time) {
        if ("generate".equals(key)) {
            int endOfBatch = Math.min(generated + batchSize, records);
            do {
                generated += 1;
                metadata.ostreams.forEach(o -> context.produceRecord(o, getRandomRecord()));
                if (generated % 100 == 0) {
                    log.debug("Generate record: " + generated + " wm " + getWatermark());
                }
            } while (generated < endOfBatch);
//            try {
//                Thread.sleep(499);
//            } catch (InterruptedException e) {
//                Thread.currentThread().interrupt();
//                throw new RuntimeException(e);
//            }
            if (generated < records) {
                context.setTimer("generate", System.currentTimeMillis());
                context.setSourceLowWatermark(getWatermark());
            } else {
                log.info("Generate record terminated: " + generated + " last wm " + getWatermark());
                context.setSourceLowWatermark(Watermark.completedOf(Watermark.ofTimestamp(targetTimestamp)).getValue());
            }
            context.askForCheckpoint();
        }
    }

    protected long getWatermark() {
        // return watermark that increment up to target
        return Watermark.ofTimestamp(targetTimestamp - (records - generated)).getValue();
    }

    protected Record getRandomRecord() {
        String msg = "data from " + metadata.name + " msg " + generated;
        Record ret = Record.of("key" + generated, msg.getBytes());
        ret.watermark = getWatermark();
        return ret;
    }
}
