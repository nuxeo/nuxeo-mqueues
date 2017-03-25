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
package org.nuxeo.ecm.platform.importer.mqueues.computation.internals;

import org.nuxeo.ecm.platform.importer.mqueues.computation.Computation;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationMetadataMapping;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationSupplier;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Stream;
import org.nuxeo.ecm.platform.importer.mqueues.computation.StreamTailer;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Streams;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Thread driving a Computation
 *
 * @since 9.1
 */
public class ComputationRunner implements Runnable {

    private final ComputationContextImpl context;
    private final int partition;
    private final Streams streams;
    private final ComputationMetadataMapping metadata;
    private final StreamTailer[] tailers;
    private final ComputationSupplier supplier;

    private Computation computation;
    private boolean stop = false;
    private long counter = 0;
    private volatile WatermarkInterval lowWatermark = new WatermarkInterval();

    public ComputationRunner(ComputationSupplier supplier, ComputationMetadataMapping metadata, int partition, Streams streams) {
        this.supplier = supplier;
        this.metadata = metadata;
        this.streams = streams;
        this.partition = partition;
        this.context = new ComputationContextImpl(metadata);

        this.tailers = new StreamTailerImpl[metadata.istreams.size()];
        int i = 0;
        for (String streamName : metadata.istreams) {
            tailers[i++] = streams.getStream(streamName).createTailerForPartition(metadata.name, partition);
        }
    }

    @Override
    public void run() {
        computation = supplier.get();
        computation.init(context);
        try {
            processLoop();
        } catch (InterruptedException e) {
            // TODO: check
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.out.println("FAILURE: " + e.getClass() + " " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            computation.destroy();
        }
    }

    private void processLoop() throws InterruptedException {
        while (!stop) {
            processTimer();
            processRecord();
        }
    }

    private void processTimer() {
        Map<String, Long> timers = context.getTimers();
        if (timers.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        final boolean[] timerUpdate = {false};
        // filter and order timers
        LinkedHashMap<String, Long> sortedTimer = timers.entrySet().stream()
                .filter(entry -> entry.getValue() < now)
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        sortedTimer.entrySet().forEach(entry -> {
            context.removeTimer(entry.getKey());
            computation.processTimer(context, entry.getKey(), entry.getValue());
            timerUpdate[0] = true;
        });
        if (timerUpdate[0]) {
            checkSourceLowWatermark();
            commitIfNecessary();
        }

    }

    private void processRecord() throws InterruptedException {
        if (tailers.length == 0) {
            return;
        }
        // round robin on input stream tailers
        StreamTailer tailer = tailers[(int) (counter++ % tailers.length)];
        // TODO: this is not optimal on multiple inputs with an empty queue
        Record record = tailer.read(Duration.ofMillis(25));
        if (record != null) {
            lowWatermark.mark(record.watermark);
            String from = metadata.reverseMap(tailer.getStreamName());
            // System.out.println(metadata.name + " receive from " + from + " record: " + record);
            computation.processRecord(context, from, record);
            checkRecordFlags(record);
            checkSourceLowWatermark();
            commitIfNecessary();
        }
    }

    private void checkSourceLowWatermark() {
        long watermark = context.getSourceLowWatermark();
        if (watermark > 0) {
            lowWatermark.mark(Watermark.ofValue(watermark));
            // System.out.println(metadata.name + " set source wm " + lowWatermark);
            context.setSourceLowWatermark(0);
        }
    }

    private void checkRecordFlags(Record record) {
        if (record.flags.contains(Record.Flag.POISON_PILL)) {
            System.out.println(metadata.name + " receive POISON PILL");
            context.setCommit(true);
            stop = true;
        } else if (record.flags.contains(Record.Flag.COMMIT)) {
            context.setCommit(true);
        }
    }

    private void commitIfNecessary() {
        if (context.isCommit()) {
            sendRecords();
            saveOffsets();
            saveTimers();
            saveState();
            lowWatermark.checkpoint();
            // System.out.println("checkpoint " + metadata.name + " " + lowWatermark);
            context.setCommit(false);
        }
    }

    private void saveTimers() {
        // TODO save timers in the key value store
    }

    private void saveState() {
        // TODO save key value store
    }

    private void saveOffsets() {
        for (StreamTailer tailer : tailers) {
            tailer.commit();
        }
    }

    private void sendRecords() {
        for (String ostream : metadata.ostreams) {
            Stream stream = streams.getStream(ostream);
            for (Record record : context.getRecords(ostream)) {
                // System.out.println(metadata.name + " send record to " + ostream + " lowwm " + lowWatermark);
                if (record.watermark == 0) {
                    record.watermark = lowWatermark.getLow().getValue();
                } else if (record.watermark < lowWatermark.getLow().getValue()) {
                    System.out.println("Desorder " + metadata.name + " send older message " + record.watermark + " " + lowWatermark);
                    lowWatermark.mark(record.watermark);
                }
                stream.appendRecord(record);
            }
            context.getRecords(ostream).clear();
        }
    }

    public long getLowWatermark() {
        return lowWatermark.getLow().getValue();
    }

    public long getLowWatermarkCompleted() {
        Watermark low = lowWatermark.getLow();
        if (low != null && low.isCompleted()) {
            return low.getValue();
        }
        return 0;
    }

    public long getLowWatermarkUncompleted() {
        Watermark low = lowWatermark.getLow();
        if (low != null && ! low.isCompleted()) {
            return low.getValue();
        }
        return 0;
    }

}
