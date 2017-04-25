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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Computation;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationMetadataMapping;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.mq.StreamTailerMQ;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.Stream;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.StreamTailer;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.Streams;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Thread driving a Computation
 *
 * @since 9.2
 */
public class ComputationRunner implements Runnable {
    private static final Log log = LogFactory.getLog(ComputationRunner.class);
    private static final long STARVING_TIMEOUT_MS = 500;
    public static final Duration READ_TIMEOUT = Duration.ofMillis(25);
    private final ComputationContextImpl context;
    private final int partition;
    private final Streams streams;
    private final ComputationMetadataMapping metadata;
    private final StreamTailer[] tailers;
    private final Supplier<Computation> supplier;

    private Computation computation;
    private volatile boolean stop = false;
    private volatile boolean drain = false;
    private long counter = 0;
    private long counterRecord = 0;
    // private final WatermarkInterval lowWatermark = new WatermarkInterval();
    private final WatermarkMonotonicInterval lowWatermark = new WatermarkMonotonicInterval();
    private long lastReadTime = System.currentTimeMillis();
    private long lastTimerExecution = 0;

    public ComputationRunner(Supplier<Computation> supplier, ComputationMetadataMapping metadata, int partition, Streams streams) {
        this.supplier = supplier;
        this.metadata = metadata;
        this.streams = streams;
        this.partition = partition;
        this.context = new ComputationContextImpl(metadata);

        this.tailers = new StreamTailerMQ[metadata.istreams.size()];
        int i = 0;
        for (String streamName : metadata.istreams) {
            tailers[i++] = streams.getStream(streamName).createTailerForPartition(metadata.name, partition);
        }
    }

    public void stop() {
        log.debug(metadata.name + ": Receives Stop signal");
        stop = true;
    }

    public void drain() {
        log.debug(metadata.name + ": Receives Drain signal");
        drain = true;
    }

    @Override
    public void run() {
        computation = supplier.get();
        computation.init(context);
        try {
            processLoop();
        } catch (InterruptedException e) {
            // this is expected when the pool is shutdownNow
            if (log.isTraceEnabled()) {
                log.debug(metadata.name + ": Interrupted", e);
            } else {
                log.debug(metadata.name + ": Interrupted");
            }
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            if (Thread.currentThread().isInterrupted()) {
                // this can happen when pool is shutdownNow throwing ClosedByInterruptException
                log.info(metadata.name + ": Interrupted", e);
            } else {
                log.error(metadata.name + ": Exception in processLoop: " + e.getMessage(), e);
                throw e;
            }
        } finally {
            computation.destroy();
            log.debug(metadata.name + ": Exited");
        }
    }

    private void processLoop() throws InterruptedException {
        while (continueLoop()) {
            processTimer();
            processRecord();
        }
    }

    private boolean continueLoop() {
        if (stop || Thread.currentThread().isInterrupted()) {
            return false;
        } else if (drain) {
            long now = System.currentTimeMillis();
            // for a source we take lastTimerExecution starvation
            if (metadata.istreams.isEmpty()) {
                if (lastTimerExecution > 0 && (now - lastTimerExecution) > STARVING_TIMEOUT_MS) {
                    log.debug(metadata.name + ": End of source drain, last timer " + STARVING_TIMEOUT_MS + " ms ago");
                    return false;
                }
            } else {
                if ((now - lastReadTime) > STARVING_TIMEOUT_MS) {
                    log.debug(metadata.name + ": End of drain no more input after " + (now - lastReadTime) +
                            " ms, " + counterRecord + " records readed, " + counter + " reads attempt");
                    return false;
                }
            }
        }
        return true;
    }

    private void processTimer() throws InterruptedException {
        Map<String, Long> timers = context.getTimers();
        if (timers.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        final boolean[] timerUpdate = {false};
        // filter and order timers
        LinkedHashMap<String, Long> sortedTimer = timers.entrySet().stream()
                .filter(entry -> entry.getValue() <= now)
                .sorted(Map.Entry.comparingByValue())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
        sortedTimer.forEach((key, value) -> {
            context.removeTimer(key);
            computation.processTimer(context, key, value);
            timerUpdate[0] = true;
        });
        if (timerUpdate[0]) {
            checkSourceLowWatermark();
            checkpointIfNecessary();
            lastTimerExecution = now;
        }

    }

    private void processRecord() throws InterruptedException {
        if (tailers.length == 0) {
            return;
        }
        // round robin on input stream tailers
        StreamTailer tailer = tailers[(int) (counter++ % tailers.length)];

        Duration timeoutRead = getTimeoutDuration();
        Record record = tailer.read(timeoutRead);
        if (record != null) {
            lastReadTime = System.currentTimeMillis();
            counterRecord++;
            lowWatermark.mark(record.watermark);
            String from = metadata.reverseMap(tailer.getStreamName());
            // System.out.println(metadata.name + ": Receive from " + from + " record: " + record);
            computation.processRecord(context, from, record);
            checkRecordFlags(record);
            checkSourceLowWatermark();
            checkpointIfNecessary();
        }
    }

    private Duration getTimeoutDuration() {
        // Adapt the duration so we are not throttling when one of the input stream is empty
        return Duration.ofMillis(Math.min(READ_TIMEOUT.toMillis(), System.currentTimeMillis() - lastReadTime));
    }

    private void checkSourceLowWatermark() {
        long watermark = context.getSourceLowWatermark();
        if (watermark > 0) {
            lowWatermark.mark(Watermark.ofValue(watermark));
            // System.out.println(metadata.name + ": Set source wm " + lowWatermark);
            context.setSourceLowWatermark(0);
        }
    }

    private void checkRecordFlags(Record record) {
        if (record.flags.contains(Record.Flag.POISON_PILL)) {
            log.debug(metadata.name + ": Receive POISON PILL");
            context.askForCheckpoint();
            stop = true;
        } else if (record.flags.contains(Record.Flag.COMMIT)) {
            context.askForCheckpoint();
        }
    }

    private void checkpointIfNecessary() throws InterruptedException {
        if (context.requireCheckpoint()) {
            boolean completed = false;
            try {
                checkpoint();
                completed = true;
            } finally {
                if (!completed) {
                    log.error(metadata.name + ": CHECKPOINT FAILURE: Resume may create duplicates.");
                }
            }
        }
    }

    private void checkpoint() throws InterruptedException {
        sendRecords();
        saveTimers();
        saveState();
        // Simulate slow checkpoint
//        try {
//            Thread.sleep(1);
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            throw e;
//        }
        saveOffsets();
        lowWatermark.checkpoint();
        // System.out.println("checkpoint " + metadata.name + " " + lowWatermark);
        context.removeCheckpointFlag();
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
                    if (log.isTraceEnabled()) {
                        log.trace(metadata.name + ": Send record in DISORDER " + record.watermark + " " + lowWatermark);
                    }
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
        return low.isCompleted() ? low.getValue() : 0;
    }

    public long getLowWatermarkUncompleted() {
        Watermark low = lowWatermark.getLow();
        return low.isCompleted() ? 0 : low.getValue();
    }
}
