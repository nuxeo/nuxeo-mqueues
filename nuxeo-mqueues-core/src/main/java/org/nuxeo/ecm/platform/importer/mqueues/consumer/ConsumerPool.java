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

import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Callable that run concurrent ConsumerCallable.
 *
 * @since 9.1
 */
public class ConsumerPool<M extends Message> implements Callable<List<ConsumerStatus>> {
    private static final Log log = LogFactory.getLog(ConsumerPool.class);
    private final MQueues<M> qm;
    private final ConsumerFactory<M> factory;
    private final BatchPolicy batchPolicy;
    private final RetryPolicy retryPolicy;

    private ExecutorService consumerExecutor;
    private ExecutorCompletionService<ConsumerStatus> consumerCompletionService;

    public ConsumerPool(MQueues<M> qm, ConsumerFactory<M> factory, BatchPolicy batchPolicy, RetryPolicy retryPolicy) {
        this.qm = qm;
        this.factory = factory;
        this.batchPolicy = batchPolicy;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public List<ConsumerStatus> call() throws Exception {
        // one thread per queue
        start();
        return waitForCompletion();
    }

    public void start() {
        int nbThreads = qm.size();
        log.warn("Running consumers with " + factory.getClass().getSimpleName() + " on " + nbThreads + " thread(s).");
        consumerExecutor = Executors.newFixedThreadPool(nbThreads, new NamedThreadFactory());
        consumerCompletionService = new ExecutorCompletionService<>(consumerExecutor);
        for (int i = 0; i < nbThreads; i++) {
            ConsumerCallable<M> callable = new ConsumerCallable<>(factory, qm.getTailer(i), batchPolicy, retryPolicy);
            consumerCompletionService.submit(callable);
        }
        log.info("All consumers are running");
    }

    public List<ConsumerStatus> waitForCompletion() {
        int nbThreads = qm.size();
        List<ConsumerStatus> ret = new ArrayList<>(nbThreads);
        for (int i = 0; i < nbThreads; i++) {
            try {
                Future<ConsumerStatus> future = consumerCompletionService.take();
                ConsumerStatus status = future.get();
                ret.add(status);
                logStat(status);
            } catch (ExecutionException e) {
                log.error("Exception catch in consumer: " + e.getMessage(), e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("ConsumerPool interrupted: " + e.getMessage(), e);
            }
        }
        logStat(ret);
        consumerExecutor.shutdownNow();
        return ret;
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger count = new AtomicInteger(0);

        @SuppressWarnings("NullableProblems")
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, String.format("Nuxeo-Consumer-%02d", count.getAndIncrement()));
        }
    }

    private void logStat(List<ConsumerStatus> ret) {
        long startTime = ret.stream().mapToLong(r -> r.startTime).min().getAsLong();
        long stopTime = ret.stream().mapToLong(r -> r.stopTime).min().getAsLong();
        double elapsed = (stopTime - startTime) / 1000.;
        long committed = ret.stream().mapToLong(r -> r.committed).sum();
        double mps = (elapsed != 0) ? committed / elapsed : 0.0;
        int consumers = ret.size();
        log.warn(String.format("All consumers terminated: %d consumers: committed: %d, elapsed: %.2fs, throughput: %.2f msg/s", consumers, committed, elapsed, mps));
    }

    private void logStat(ConsumerStatus status) {
        double elapsed = (status.stopTime - status.startTime) / 1000.;
        double mps = (elapsed != 0) ? status.committed / elapsed : 0.0;
        log.info(String.format("Stat consumer %02d terminated, accepted (include retries): %d, committed: %d, batch: %d, batchFailure: %d, elapsed: %.2fs, throughput: %.2f msg/s.",
                status.consumer, status.accepted, status.committed, status.batchCommit, status.batchFailure, elapsed, mps));
    }
}
