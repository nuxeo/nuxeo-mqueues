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

import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationMetadataMapping;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationSupplier;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Stream;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Streams;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * Pool of ComputationRunner
 *
 * @since 9.1
 */
public class ComputationPool {
    private final ComputationMetadataMapping metadata;
    private final int threads;
    private final Streams streams;
    private final ComputationSupplier supplier;
    private ExecutorService threadPool;
    private List<ComputationRunner> runners;

    public ComputationPool(ComputationSupplier supplier, ComputationMetadataMapping metadata, int defaultThreads, Streams streams) {
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
        throw new IllegalArgumentException("No imput stream");
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
        System.out.println(metadata.name + " pool started with " + threads + " threads");
    }

    public void stop() {
        if (threadPool != null && !threadPool.isTerminated()) {
            threadPool.shutdownNow();
        }
        runners.clear();
    }

    public long getLowWatermark() {
        // 1. low value of uncompleted job that are not 0
        long low = runners.stream().map(ComputationRunner::getLowWatermarkUncompleted).filter(wm -> wm > 0).min(Comparator.naturalOrder()).orElse(0L);
        if (low > 0) {
            return low;
        }
        // 2. max value of completed job that are not null
        low = runners.stream().map(ComputationRunner::getLowWatermarkCompleted).max(Comparator.naturalOrder()).orElse(0L);
        return low;
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
            t.setUncaughtExceptionHandler((t1, e) -> System.out.println("Uncaught error on thread " + t1.getName() + " " + e.getMessage()));
            return t;
        }
    }

}
