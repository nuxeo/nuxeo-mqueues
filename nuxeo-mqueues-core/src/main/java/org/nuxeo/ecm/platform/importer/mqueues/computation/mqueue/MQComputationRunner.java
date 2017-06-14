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
package org.nuxeo.ecm.platform.importer.mqueues.computation.mqueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Computation;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationMetadataMapping;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.ComputationContextImpl;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.WatermarkMonotonicInterval;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRebalanceException;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRebalanceListener;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRecord;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Thread driving a Computation
 *
 * @since 9.2
 */
public class MQComputationRunner implements Runnable, MQRebalanceListener {
    private static final Log log = LogFactory.getLog(MQComputationRunner.class);
    private static final long STARVING_TIMEOUT_MS = 1000;
    public static final Duration READ_TIMEOUT = Duration.ofMillis(25);

    private final ComputationContextImpl context;
    private final MQManager<Record> mqManager;
    private final ComputationMetadataMapping metadata;
    private final MQTailer<Record> tailer;
    private final Supplier<Computation> supplier;

    private volatile boolean stop = false;
    private volatile boolean drain = false;

    private Computation computation;
    private long counter = 0;
    private long inRecords = 0;
    private long inCheckpointRecords = 0;
    private long outRecords = 0;
    private final WatermarkMonotonicInterval lowWatermark = new WatermarkMonotonicInterval();
    private long lastReadTime = System.currentTimeMillis();
    private long lastTimerExecution = 0;
    private String threadName;

    @SuppressWarnings("unchecked")
    public MQComputationRunner(Supplier<Computation> supplier, ComputationMetadataMapping metadata,
                               List<MQPartition> defaultAssignment, MQManager<Record> mqManager) {
        this.supplier = supplier;
        this.metadata = metadata;
        this.mqManager = mqManager;
        this.context = new ComputationContextImpl(metadata);
        if (metadata.istreams.isEmpty()) {
            this.tailer = null;
        } else if (mqManager.supportSubscribe()) {
            this.tailer = mqManager.subscribe(metadata.name, metadata.istreams, this);
        } else {
            this.tailer = mqManager.createTailer(metadata.name, defaultAssignment);
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
        threadName = Thread.currentThread().getName();
        boolean interrupted = false;
        computation = supplier.get();
        log.debug(metadata.name + ": Init");
        computation.init(context);
        log.debug(metadata.name + ": Start");
        try {
            processLoop();
        } catch (InterruptedException e) {
            // this is expected when the pool is shutdownNow
            if (log.isTraceEnabled()) {
                log.debug(metadata.name + ": Interrupted", e);
            } else {
                log.debug(metadata.name + ": Interrupted");
            }
            // the interrupt flag is set after the tailer are closed
            interrupted = true;
        } catch (Exception e) {
            if (Thread.currentThread().isInterrupted()) {
                // this can happen when pool is shutdownNow throwing ClosedByInterruptException
                log.info(metadata.name + ": Interrupted", e);
            } else {
                log.error(metadata.name + ": Exception in processLoop: " + e.getMessage(), e);
                throw e;
            }
        } finally {
            try {
                computation.destroy();
                closeTailer();
                log.debug(metadata.name + ": Exited");
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void closeTailer() {
        if (tailer != null && !tailer.closed()) {
            try {
                tailer.close();
            } catch (Exception e) {
                log.debug(metadata.name + ": Exception while closing tailer", e);
            }
        }
    }

    private void processLoop() throws InterruptedException {
        while (continueLoop()) {
            processTimer();
            processRecord();
            // TODO: add pause for computation without inputs or without timer to prevent CPU hogs
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
                    log.info(metadata.name + ": End of source drain, last timer " + STARVING_TIMEOUT_MS + " ms ago");
                    return false;
                }
            } else {
                if ((now - lastReadTime) > STARVING_TIMEOUT_MS) {
                    log.info(metadata.name + ": End of drain no more input after " + (now - lastReadTime) +
                            " ms, " + inRecords + " records read, " + counter + " reads attempt");
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
            lastTimerExecution = now;
            setThreadName("timer");
            checkpointIfNecessary();
        }

    }

    private void processRecord() throws InterruptedException {
        if (tailer == null) {
            return;
        }
        Duration timeoutRead = getTimeoutDuration();
        MQRecord<Record> mqRecord = null;
        try {
            mqRecord = tailer.read(timeoutRead);
        } catch (MQRebalanceException e) {
            // the revoke has done a checkpoint we can continue
        }
        Record record;
        if (mqRecord != null) {
            record = mqRecord.value();
            lastReadTime = System.currentTimeMillis();
            inRecords++;
            lowWatermark.mark(record.watermark);
            String from = metadata.reverseMap(mqRecord.partition().name());
            // System.out.println(metadata.name + ": Receive from " + from + " record: " + record);
            computation.processRecord(context, from, record);
            checkRecordFlags(record);
            checkSourceLowWatermark();
            setThreadName("record");
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
            log.info(metadata.name + ": Receive POISON PILL");
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
                inCheckpointRecords = inRecords;
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
        log.debug(metadata.name + ": checkpoint");
        setThreadName("checkpoint");
    }

    private void saveTimers() {
        // TODO save timers in the key value store
    }

    private void saveState() {
        // TODO save key value store
    }

    private void saveOffsets() {
        if (tailer != null) {
            tailer.commit();
        }
    }

    private void sendRecords() {
        for (String ostream : metadata.ostreams) {
            MQAppender<Record> appender = mqManager.getAppender(ostream);
            for (Record record : context.getRecords(ostream)) {
                // System.out.println(metadata.name + " send record to " + ostream + " lowwm " + lowWatermark);
                if (record.watermark == 0) {
                    // use low watermark when not set
                    record.watermark = lowWatermark.getLow().getValue();
                }
                appender.append(record.key, record);
                outRecords++;
            }
            context.getRecords(ostream).clear();
        }
    }

    public Watermark getLowWatermark() {
        return lowWatermark.getLow();
    }

    private void setThreadName(String message) {
        String name = threadName + ",in:" + inRecords + ",inCheckpoint:" + inCheckpointRecords + ",out:" + outRecords +
                ",lastRead:" + lastReadTime +
                ",lastTimer:" + lastTimerExecution + ",wm:" + lowWatermark.getLow().getValue() +
                ",loop:" + counter;
        if (message != null) {
            name += "," + message;
        }
        Thread.currentThread().setName(name);
    }

    @Override
    public void onPartitionsRevoked(Collection<MQPartition> partitions) {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            checkpoint();
            setThreadName("rebalance revoked");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<MQPartition> partitions) {
        lastReadTime = System.currentTimeMillis();
        setThreadName("rebalance assigned");
    }
}
