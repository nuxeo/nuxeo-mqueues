package org.nuxeo.ecm.platform.importer.mqueues.computation;/*
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

import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.ComputationPool;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @since 9.1
 */
public class ComputationManager {
    private final Streams streams;
    private final Topology topology;
    private final Settings settings;
    private final List<ComputationPool> pools;

    public ComputationManager(Streams streams, Topology topology, Settings settings) {
        this.streams = streams;
        this.topology = topology;
        this.settings = settings;
        initStreams();
        this.pools = initPools();
        Objects.requireNonNull(pools);
    }

    /**
     * Run the computations
     */
    public void start() {
        pools.forEach(ComputationPool::start);
    }

    /**
     * Stop computations gracefully after processing a record or a timer.
     */
    public boolean stop(Duration timeout) {
        long failures = pools.parallelStream().filter(comp -> !comp.stop(timeout)).count();
        return failures == 0L;
    }

    public boolean stop() {
        return stop(Duration.ofSeconds(1));
    }

    /**
     * Stop computations when input streams are empty.
     * The timeout is applied for each computation, the total duration can be up to nb computations * timeout
     * Returns true if computations are stopped during the timeout delay.
     */
    public boolean drainAndStop(Duration timeout) {
        // here the order matters, this must be done sequentially
        long  failures = pools.stream().filter(comp -> !comp.drainAndStop(timeout)).count();
        return failures == 0L;
    }

    /**
     * Shutdown immediately
     */
    public void shutdown() {
        pools.parallelStream().forEach(ComputationPool::shutdown);
    }


    public long getLowWatermark() {
        // System.out.println("Low watermark ...");
        // topology.metadataList().forEach(meta -> System.out.println("  low " + meta.name + " : \t" + getLowWatermark(meta.name)));
        long ret = pools.stream().map(ComputationPool::getLowWatermark).min(Comparator.naturalOrder()).get();
        // System.out.println("Low watermark ----------- is " + ret);
        return ret;
    }

    public long getLowWatermark(String computationName) {
        Objects.nonNull(computationName);
        return pools.stream().filter(pool -> computationName.equals(pool.getComputationName())).map(ComputationPool::getLowWatermark).min(Comparator.naturalOrder()).get();
    }

    public boolean isDone(long timestamp) {
        return Watermark.ofValue(getLowWatermark()).isDone(timestamp);
    }


    public String toPlantuml() {
        return topology.toPlantuml(settings);
    }

    private List<ComputationPool> initPools() {
        return topology.metadataList().stream()
                .map(meta -> new ComputationPool(topology.getSupplier(meta.name), meta, settings.getConcurrency(meta.name), streams))
                .collect(Collectors.toList());
    }

    private void initStreams() {
        Map<String, Integer> streamPartitions = computePartitions();
        topology.streamsSet().forEach(name -> streams.getOrCreateStream(name, streamPartitions.get(name)));
    }

    private Map<String, Integer> computePartitions() {
        Map<String, Integer> ret = new HashMap<>();
        // set input stream partition according to computation concurrency
        topology.metadataList().forEach(meta -> meta.istreams.forEach(
                stream -> ret.put(stream, settings.getConcurrency(meta.name))));
        // set default on external output streams
        topology.streamsSet().forEach(stream -> ret.putIfAbsent(stream, settings.getConcurrency(stream)));
        // check if there is a conflict when input stream partition does not match computation concurrency
        Set<ComputationMetadataMapping> conflicts = topology.metadataList().stream().filter(meta -> checkConflict(meta.name, ret)).collect(Collectors.toSet());
        if (!conflicts.isEmpty()) {
            throw new IllegalArgumentException("Conflict detected: " + conflicts.stream().map(meta -> "Computation " + meta + " can not apply concurrency of: " + settings.getConcurrency(meta.name)).collect(Collectors.joining(". ")));
        }
        return ret;
    }

    private boolean checkConflict(String name, Map<String, Integer> ret) {
        return topology.getParents(name).stream().filter(stream -> ret.get(stream) != settings.getConcurrency(name)).count() > 0;
    }

}
