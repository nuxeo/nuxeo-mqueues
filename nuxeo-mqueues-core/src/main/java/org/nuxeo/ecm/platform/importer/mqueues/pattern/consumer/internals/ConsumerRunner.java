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
package org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.internals;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import net.jodah.failsafe.Execution;
import net.openhft.chronicle.core.util.Time;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQRecord;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.Message;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.BatchPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.Consumer;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerFactory;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerPolicy;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.consumer.ConsumerStatus;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Thread.currentThread;

/**
 * Read messages from a tailer and drive a consumer according to its policy.
 *
 * @since 9.1
 */
public class ConsumerRunner<M extends Message> implements Callable<ConsumerStatus> {
    private static final Log log = LogFactory.getLog(ConsumerRunner.class);

    // This is the registry name used by Nuxeo without adding a dependency nuxeo-runtime
    public static final String NUXEO_METRICS_REGISTRY_NAME = "org.nuxeo.runtime.metrics.MetricsService";

    private final ConsumerFactory<M> factory;
    private final ConsumerPolicy policy;
    private final int queue;
    private final MQTailer<M> tailer;
    private BatchPolicy currentBatchPolicy;
    private String threadName;
    private Consumer<M> consumer;

    protected final MetricRegistry registry = SharedMetricRegistries.getOrCreate(NUXEO_METRICS_REGISTRY_NAME);
    protected final Timer acceptTimer;
    protected final Counter committedCounter;
    protected final Timer batchCommitTimer;
    protected final Counter batchFailureCount;
    protected final Counter consumersCount;


    public ConsumerRunner(ConsumerFactory<M> factory, ConsumerPolicy policy, MQTailer<M> tailer) {
        this.factory = factory;
        this.tailer = tailer;
        this.currentBatchPolicy = policy.getBatchPolicy();
        this.policy = policy;
        queue = tailer.getQueue();
        consumersCount = newCounter(MetricRegistry.name("nuxeo", "importer", "queue", "consumers"));
        acceptTimer = newTimer(MetricRegistry.name("nuxeo", "importer", "queue", "consumer", "accepted", String.valueOf(queue)));
        committedCounter = newCounter(MetricRegistry.name("nuxeo", "importer", "queue", "consumer", "committed", String.valueOf(queue)));
        batchFailureCount = newCounter(MetricRegistry.name("nuxeo", "importer", "queue", "consumer", "batchFailure", String.valueOf(queue)));
        batchCommitTimer = newTimer(MetricRegistry.name("nuxeo", "importer", "queue", "consumer", "batchCommit", String.valueOf(queue)));
        log.debug("Consumer thread created tailing on queue: " + queue);
    }

    private Counter newCounter(String name) {
        registry.remove(name);
        return registry.counter(name);
    }

    private Timer newTimer(String name) {
        registry.remove(name);
        return registry.timer(name);
    }

    @Override
    public ConsumerStatus call() throws Exception {
        threadName = currentThread().getName();
        consumersCount.inc();
        long start = Time.currentTimeMillis();
        setTailerPosition();
        consumer = factory.createConsumer(queue);
        try {
            addSalt();
            consumerLoop();
        } finally {
            consumer.close();
            consumersCount.dec();
        }
        return new ConsumerStatus(queue, acceptTimer.getCount(), committedCounter.getCount(),
                batchCommitTimer.getCount(), batchFailureCount.getCount(), start, Time.currentTimeMillis(), false);
    }

    private void addSalt() throws InterruptedException {
        long randomDelay = ThreadLocalRandom.current().nextLong(policy.getBatchPolicy().getTimeThreshold().toMillis());
        if (policy.isSalted()) {
            Thread.sleep(randomDelay);
        }
    }

    private void setTailerPosition() {
        switch (policy.getStartOffset()) {
            case BEGIN:
                tailer.toStart();
                break;
            case END:
                tailer.toEnd();
                break;
            default:
                tailer.toLastCommitted();
        }
    }

    private void consumerLoop() throws InterruptedException {
        boolean end = false;
        while (!end) {
            Execution execution = new Execution(policy.getRetryPolicy());
            end = processBatchWithRetry(execution);
            if (execution.getLastFailure() != null) {
                if (policy.continueOnFailure()) {
                    log.error("Skip message on failure after applying the retry policy: ", execution.getLastFailure());
                } else {
                    log.error("Abort on Failure after applying the retry policy: ", execution.getLastFailure());
                    end = true;
                }
            }
        }
    }

    private boolean processBatchWithRetry(Execution execution) throws InterruptedException {
        boolean end = false;
        while (!execution.isComplete()) {
            try {
                end = processBatch();
                execution.complete();
                tailer.commit();
            } catch (Throwable t) {
                batchFailureCount.inc();
                if (!execution.canRetryOn(t)) {
                    if (t instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    throw t;
                }
                setBatchRetryPolicy();
                tailer.toLastCommitted();
            }
            restoreBatchPolicy();
        }
        return end;
    }

    private void setBatchRetryPolicy() {
        currentBatchPolicy = BatchPolicy.NO_BATCH;
    }

    private void restoreBatchPolicy() {
        currentBatchPolicy = policy.getBatchPolicy();
    }

    private boolean processBatch() throws InterruptedException {
        boolean end = false;
        beginBatch();
        try {
            BatchState state = acceptBatch();
            commitBatch(state);
            if (state.getState() == BatchState.State.LAST) {
                log.info(String.format("No more message on queue %02d", queue));
                end = true;
            }
        } catch (Exception e) {
            try {
                rollbackBatch();
            } catch (Exception rollbackException) {
                log.error("Exception on rollback invocation", rollbackException);
                // we propagate the initial error.
            }
            throw e;
        }
        return end;
    }

    private void beginBatch() {
        consumer.begin();
    }

    private void commitBatch(BatchState state) {
        try (Timer.Context ignore = batchCommitTimer.time()) {
            consumer.commit();
            committedCounter.inc(state.getSize());
        }
    }

    private void rollbackBatch() {
        log.warn("Rollback batch");
        consumer.rollback();
    }

    private BatchState acceptBatch() throws InterruptedException {
        BatchState batch = new BatchState(currentBatchPolicy);
        batch.start();
        MQRecord<M> record;
        M message;
        while ((record = tailer.read(policy.getWaitMessageTimeout())) != null) {
            message = record.value;
            if (message.poisonPill()) {
                log.warn("Receive a poison pill: " + message);
                batch.last();
            } else {
                try (Timer.Context ignore = acceptTimer.time()) {
                    setThreadName(message);
                    consumer.accept(message);
                }
                batch.inc();
                if (message.forceBatch()) {
                    if (log.isDebugEnabled()) {
                        log.debug("Force end of batch: " + message);
                    }
                    batch.force();
                }
            }
            if (batch.getState() != BatchState.State.FILLING) {
                return batch;
            }
        }
        batch.last();
        return batch;
    }

    private void setThreadName(M message) {
        String name = threadName + "-" + acceptTimer.getCount();
        if (message != null) {
            name += "-" + message.getId();
        } else {
            name += "-null";
        }
        currentThread().setName(name);
    }
}