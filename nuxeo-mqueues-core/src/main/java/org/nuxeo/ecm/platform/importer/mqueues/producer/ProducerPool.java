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
package org.nuxeo.ecm.platform.importer.mqueues.producer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.platform.importer.mqueues.message.Message;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Callable that run concurrent ProducerLoop.
 *
 * @since 9.1
 */
public class ProducerPool<M extends Message> implements Callable<List<ProducerStatus>> {
    private static final Log log = LogFactory.getLog(ProducerPool.class);
    private final int nbThreads;
    private CompletionService<ProducerStatus> producerCompletionService;
    private ExecutorService producerExecutor;
    private final MQueues<M> mq;
    private final ProducerFactory<M> factory;

    private static class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger count = new AtomicInteger(0);

        @SuppressWarnings("NullableProblems")
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, String.format("Nuxeo-ProducerIterator-%02d", count.getAndIncrement()));
        }
    }

    public ProducerPool(MQueues<M> mq, ProducerFactory<M> factory, int nbThreads) {
        this.mq = mq;
        this.factory = factory;
        this.nbThreads = nbThreads;
    }

    @Override
    public List<ProducerStatus> call() throws Exception {
        start();
        return waitForCompletion();
    }


    public void start() {
        log.warn("Running producers with " + factory.getClass().getSimpleName() + " on " + nbThreads + " thread(s).");
        producerExecutor = Executors.newFixedThreadPool(nbThreads, new NamedThreadFactory());
        producerCompletionService = new ExecutorCompletionService<>(producerExecutor);
        for (int i = 0; i < nbThreads; i++) {
            ProducerLoop<M> callable = new ProducerLoop<>(factory, mq, i);
            producerCompletionService.submit(callable);
        }
        log.info("All producers are running");
    }

    public List<ProducerStatus> waitForCompletion() {
        List<ProducerStatus> ret = new ArrayList<>(nbThreads);
        try {
            for (int i = 0; i < nbThreads; i++) {
                Future<ProducerStatus> future = producerCompletionService.take();
                ProducerStatus status = future.get();
                ret.add(status);
                log.info(String.format("ProducerIterator %d terminated, produced: %d, elapsed: %.2fs.",
                        status.producer, status.nbProcessed, (status.stopTime - status.startTime) / 1000.));
            }
        } catch (ExecutionException e) {
            log.error("Exception catch in producer " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("ProducerPool interrupted " + e.getMessage(), e);
        }
        logStat(ret);
        producerExecutor.shutdownNow();
        return ret;
    }

    private void logStat(List<ProducerStatus> ret) {
        long startTime = ret.stream().mapToLong(r -> r.startTime).min().orElse(0);
        long stopTime = ret.stream().mapToLong(r -> r.stopTime).min().orElse(0);
        double elapsed = (stopTime - startTime) / 1000.;
        long committed = ret.stream().mapToLong(r -> r.nbProcessed).sum();
        double mps = (elapsed != 0) ? committed / elapsed : 0.0;
        int producers = ret.size();
        log.warn(String.format("All %d producers terminated: messages committed: %d, elapsed: %.2fs, throughput: %.2f msg/s, queues: %d",
                producers, committed, elapsed, mps, mq.size()));
    }

}
