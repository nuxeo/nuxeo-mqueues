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
package org.nuxeo.ecm.platform.importer.mqueues.consumer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import net.jodah.failsafe.Execution;
import net.openhft.chronicle.core.util.Time;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.currentThread;

/**
 * Run a consumer according to the batch and retry policy until there is no more message.
 *
 * @since 9.1
 */
public class ConsumerRunner<M extends Message> implements Callable<ConsumerStatus> {
    private static final Log log = LogFactory.getLog(ConsumerRunner.class);

    // This is the registry name used by Nuxeo without adding a dependency nuxeo-runtime
    public static final String NUXEO_METRICS_REGISTRY_NAME = "org.nuxeo.runtime.metrics.MetricsService";

    private final Consumer<M> consumer;
    private final MQueues<M> mq;
    private final int queue;
    private final ConsumerPolicy policy;
    private BatchPolicy currentBatchPolicy;
    private String threadName;

    protected final MetricRegistry registry = SharedMetricRegistries.getOrCreate(NUXEO_METRICS_REGISTRY_NAME);
    protected final Timer acceptTimer;
    protected final Counter committedCounter;
    protected final Timer batchCommitTimer;
    protected final Counter batchFailureCount;
    protected final Counter consumersCount;
    private MQueues.Tailer<M> tailer;

    public ConsumerRunner(ConsumerFactory<M> factory, MQueues<M> mq, int queue, ConsumerPolicy policy) {
        this.consumer = factory.createConsumer(queue);
        this.mq = mq;
        this.queue = queue;
        this.currentBatchPolicy = policy.getBatchPolicy();
        this.policy = policy;

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
        consumersCount.inc();
        threadName = currentThread().getName();
        long start = Time.currentTimeMillis();
        tailer = mq.createTailer(queue);
        switch(policy.getStartOffset()) {
            case BEGIN:
                tailer.toStart();
                break;
            case END:
                tailer.toEnd();
                break;
            default:
                tailer.toLastCommitted();
        }
        try {
            consumerLoop();
        } finally {
            tailer.close();
            consumer.close();
            consumersCount.dec();
        }
        return new ConsumerStatus(queue, acceptTimer.getCount(), committedCounter.getCount(),
                batchCommitTimer.getCount(), batchFailureCount.getCount(), start, Time.currentTimeMillis());
    }

    private void consumerLoop() {
        boolean end = false;
        do {
            Execution execution = new Execution(policy.getRetryPolicy());
            while (!execution.isComplete()) {
                try {
                    end = processBatch();
                    execution.complete();
                    tailer.commit();
                } catch (Exception e) {
                    execution.recordFailure(e);
                    setBatchRetryPolicy();
                    tailer.toLastCommitted();
                    batchFailureCount.inc();
                }
            }
            if (execution.getLastFailure() != null) {
                if (policy.continueOnFailure()) {
                    log.error("Skip message on failure after applying the retry policy: ", execution.getLastFailure());
                } else {
                    log.error("Abort on Failure after applying the retry policy: ", execution.getLastFailure());
                    end = true;
                }
            }
            restoreBatchPolicy();
        } while (!end);
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
        M message;
        while ((message = tailer.read(policy.getWaitForMessageMs(), TimeUnit.MILLISECONDS)) != null) {
            try (Timer.Context ignore = acceptTimer.time()) {
                setThreadName(message);
                consumer.accept(message);
            }
            if (batch.inc() != BatchState.State.FILLING) {
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