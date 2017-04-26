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
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.Stream;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.Streams;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * Pool of ComputationRunner
 *
 * @since 9.2
 */
public class ComputationPool {
    private static final Log log = LogFactory.getLog(ComputationPool.class);
    private final ComputationMetadataMapping metadata;
    private final int threads;
    private final Streams streams;
    private final Supplier<Computation> supplier;
    private ExecutorService threadPool;
    private final List<ComputationRunner> runners;

    public ComputationPool(Supplier<Computation> supplier, ComputationMetadataMapping metadata, int defaultThreads, Streams streams) {
        this.supplier = supplier;
        this.streams = streams;
        this.metadata = metadata;
        this.threads = getNumberOfThreads(defaultThreads);
        this.runners = new ArrayList<>(defaultThreads);
    }

    public String getComputationName() {
        return metadata.name;
    }

    private int getNumberOfThreads(int defaultThreads) {
        if (metadata.istreams.isEmpty()) {
            return defaultThreads;
        }
        return getPartitionsOfInputStreams();
    }

    private int getPartitionsOfInputStreams() {
        for (String streamName : metadata.istreams) {
            Stream stream = streams.getStream(streamName);
            return stream.getPartitions();
        }
        throw new IllegalArgumentException("No input stream");
    }

    public void start() {
        threadPool = newFixedThreadPool(threads, new NamedThreadFactory(metadata.name + "Pool"));
        for (int i = 0; i < threads; i++) {
            ComputationRunner runner = new ComputationRunner(supplier, metadata, i, streams);
            threadPool.submit(runner);
            runners.add(runner);
        }
        // close the pool no new admission
        threadPool.shutdown();
        log.debug("Pool started for " + metadata.name + " size: " + threads);
    }

    public boolean drainAndStop(Duration timeout) {
        if (threadPool == null || threadPool.isTerminated()) {
            return true;
        }
        runners.forEach(ComputationRunner::drain);
        boolean ret = awaitPoolTermination(timeout);
        stop(Duration.ofSeconds(1));
        return ret;
    }

    public boolean stop(Duration timeout) {
        if (threadPool == null || threadPool.isTerminated()) {
            return true;
        }
        runners.forEach(ComputationRunner::stop);
        boolean ret = awaitPoolTermination(timeout);
        shutdown();
        return ret;
    }

    public void shutdown() {
        if (threadPool != null && !threadPool.isTerminated()) {
            threadPool.shutdownNow();
            // give a chance to end threads with valid tailers when shutdown is followed by streams.close()
            try {
                threadPool.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn(metadata.name + ": Interrupted in shutdown");
                Thread.currentThread().interrupt();
            }
        }
        runners.clear();
        threadPool = null;
    }

    private boolean awaitPoolTermination(Duration timeout) {
        try {
            if (!threadPool.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn(metadata.name + ": Timeout on wait for pool termination");
                return false;
            }
        } catch (InterruptedException e) {
            log.warn(metadata.name + ": Interrupted while waiting for pool termination");
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    public long getLowWatermark() {
        // Take the lowest positive watermark of unprocessed records for the pool
        long ret = runners.stream().map(ComputationRunner::getLowWatermarkUncompleted).filter(wm -> wm > 0)
                .min(Comparator.naturalOrder()).orElse(0L);
        if (ret == 0) {
            // There is no pending records, the low watermark is the highest completed watermark
            // (filter is used to remove the special case of 1 which is the completed value of 0)
            ret = runners.stream().map(ComputationRunner::getLowWatermarkCompleted).filter(wm -> wm > 1)
                    .max(Comparator.naturalOrder()).orElse(0L);
        }
        return ret;
    }

    protected static class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger count = new AtomicInteger(0);
        private final String prefix;

        public NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @SuppressWarnings("NullableProblems")
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, String.format("%s-%02d", prefix, count.getAndIncrement()));
            t.setUncaughtExceptionHandler((t1, e) -> log.error("Uncaught exception: " + e.getMessage(), e));
            return t;
        }
    }

}
