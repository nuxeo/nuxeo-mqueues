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
import net.jodah.failsafe.RetryPolicy;
import net.openhft.chronicle.core.util.Time;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.runtime.metrics.MetricsService;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.currentThread;

/**
 * Callable that run a consumer according to the batch and retry policy.
 *
 * @since 9.1
 */
public class ConsumerCallable<M extends Message> implements Callable<ConsumerStatus> {
    private static final Log log = LogFactory.getLog(ConsumerCallable.class);

    private final Consumer<M> consumer;
    private final MQueues.Tailer<M> tailer;
    private final RetryPolicy retryPolicy;
    private final BatchPolicy batchPolicy;
    private BatchState currentBatch;
    private BatchPolicy currentBatchPolicy;
    private String threadName;

    protected final MetricRegistry registry = SharedMetricRegistries.getOrCreate(MetricsService.class.getName());
    protected final Timer acceptTimer;
    protected final Counter committedCounter;
    protected final Timer batchCommitTimer;
    protected final Counter batchFailureCount;
    protected final Counter consumersCount;


    public ConsumerCallable(ConsumerFactory<M> factory, MQueues.Tailer<M> tailer, BatchPolicy batchPolicy, RetryPolicy retryPolicy) {
        this.consumer = factory.createConsumer(tailer.getQueue());
        this.tailer = tailer;
        this.currentBatchPolicy = this.batchPolicy = batchPolicy;
        this.retryPolicy = retryPolicy;

        consumersCount = newCounter(MetricRegistry.name("nuxeo", "importer", "queue", "consumers"));
        acceptTimer = newTimer(MetricRegistry.name("nuxeo", "importer", "queue", "consumer", "accepted", String.valueOf(tailer.getQueue())));
        committedCounter = newCounter(MetricRegistry.name("nuxeo", "importer", "queue", "consumer", "committed", String.valueOf(tailer.getQueue())));
        batchFailureCount = newCounter(MetricRegistry.name("nuxeo", "importer", "queue", "consumer", "batchFailure", String.valueOf(tailer.getQueue())));
        batchCommitTimer = newTimer(MetricRegistry.name("nuxeo", "importer", "queue", "consumer", "batchCommit", String.valueOf(tailer.getQueue())));
        log.debug("Consumer thread created tailing on queue: " + tailer.getQueue());
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
        try {
            consumerLoop();
        } finally {
            consumer.close();
            consumersCount.dec();
        }
        return new ConsumerStatus(tailer.getQueue(), acceptTimer.getCount(), committedCounter.getCount(),
                batchCommitTimer.getCount(), batchFailureCount.getCount(), start, Time.currentTimeMillis());
    }

    private void consumerLoop() {
        boolean end = false;
        do {
            Execution execution = new Execution(retryPolicy);
            while (!execution.isComplete()) {
                try {
                    end = processBatch();
                    execution.complete();
                    tailer.commit();
                } catch (Throwable e) {
                    execution.recordFailure(e);
                    setBatchRetryPolicy();
                    log.warn("Failure on batch processing, try #" + execution.getExecutions() + " " + e.getMessage(), e);
                    tailer.toLastCommitted();
                    batchFailureCount.inc();
                }
            }
            if (execution.getLastFailure() != null) {
                log.error("Abort on batchFailure of batch processing: ", execution.getLastFailure());
                end = true;
            }
            restoreBatchPolicy();
        } while (!end);
    }

    private void setBatchRetryPolicy() {
        currentBatchPolicy = BatchPolicy.NO_BATCH;
    }

    private void restoreBatchPolicy() {
        currentBatchPolicy = batchPolicy;
    }


    private boolean processBatch() throws InterruptedException {
        boolean end = false;
        beginBatch();
        try {
            end = acceptBatch();
            commitBatch();
        } catch (Exception e) {
            rollbackBatch();
            throw e;
        }
        return end;
    }


    private void beginBatch() {
        consumer.begin();
    }

    private void commitBatch() {
        try (Timer.Context ignore = batchCommitTimer.time()) {
            consumer.commit();
            committedCounter.inc(currentBatch.getSize());
        }
    }

    private void rollbackBatch() {
        log.warn("Rollback batch");
        consumer.rollback();
    }

    private boolean acceptBatch() throws InterruptedException {
        currentBatch = new BatchState(currentBatchPolicy);
        currentBatch.start();
        M message;
        while ((message = tailer.get(1, TimeUnit.SECONDS)) != null) {
            try (Timer.Context ignore = acceptTimer.time()) {
                setThreadName(message);
                consumer.accept(message);
            }
            currentBatch.inc();
            if (currentBatch.isFull()) {
                return false;
            }
        }
        log.info(String.format("No more message on queue %02d", tailer.getQueue()));
        return true;
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