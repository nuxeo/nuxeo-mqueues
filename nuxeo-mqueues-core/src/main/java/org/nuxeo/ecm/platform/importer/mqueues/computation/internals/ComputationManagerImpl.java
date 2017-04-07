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
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationManager;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationMetadataMapping;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Settings;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Topology;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.Streams;

import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @since 9.2
 */
public class ComputationManagerImpl implements ComputationManager {
    private static final Log log = LogFactory.getLog(ComputationManagerImpl.class);
    private final Streams streams;
    private final Topology topology;
    private final Settings settings;
    private final List<ComputationPool> pools;

    public ComputationManagerImpl(Streams streams, Topology topology, Settings settings) {
        this.streams = streams;
        this.topology = topology;
        this.settings = settings;
        initStreams();
        this.pools = initPools();
        Objects.requireNonNull(pools);
    }

    @Override
    public void start() {
        log.debug("Starting ...");
        pools.forEach(ComputationPool::start);
    }

    @Override
    public boolean stop(Duration timeout) {
        log.debug("Stopping");
        long failures = pools.parallelStream().filter(comp -> !comp.stop(timeout)).count();
        log.debug(String.format("Stopped %d failure", failures));
        return failures == 0L;
    }

    @Override
    public boolean stop() {
        return stop(Duration.ofSeconds(1));
    }

    @Override
    public boolean drainAndStop(Duration timeout) {
        // here the order matters, this must be done sequentially
        log.debug("Drain and stop");
        long failures = pools.stream().filter(comp -> !comp.drainAndStop(timeout)).count();
        log.debug(String.format("Drained and stopped %d failure", failures));
        return failures == 0L;
    }

    @Override
    public void shutdown() {
        log.debug("Shutdowning ...");
        pools.parallelStream().forEach(ComputationPool::shutdown);
        log.debug("Shutdowned");
    }


    @Override
    public long getLowWatermark() {
        long ret = pools.stream().map(ComputationPool::getLowWatermark).min(Comparator.naturalOrder()).get();
        if (log.isTraceEnabled()) {
            log.trace("lowWatermark: " + ret);
            // topology.metadataList().forEach(meta -> System.out.println("  low " + meta.name + " : \t" + getLowWatermark(meta.name)));
        }
        return ret;
    }

    @Override
    public long getLowWatermark(String computationName) {
        Objects.nonNull(computationName);
        return pools.stream().filter(pool -> computationName.equals(pool.getComputationName())).map(ComputationPool::getLowWatermark).min(Comparator.naturalOrder()).get();
    }

    @Override
    public boolean isDone(long timestamp) {
        return Watermark.ofValue(getLowWatermark()).isDone(timestamp);
    }

    private List<ComputationPool> initPools() {
        log.debug("Initializing pools");
        return topology.metadataList().stream()
                .map(meta -> new ComputationPool(topology.getSupplier(meta.name), meta,
                        settings.getConcurrency(meta.name), streams))
                .collect(Collectors.toList());
    }

    private void initStreams() {
        log.debug("Initializing streams");
        Map<String, Integer> streamPartitions = computePartitions();
        topology.streamsSet().forEach(name -> streams.getOrCreateStream(name, streamPartitions.get(name)));
    }

    private Map<String, Integer> computePartitions() {
        Map<String, Integer> ret = new HashMap<>();
        // set input stream partition according to computation concurrency
        topology.metadataList().forEach(meta -> meta.istreams.forEach(
                stream -> ret.put(stream, settings.getConcurrency(meta.name))));
        // set default on external output streams
        topology.streamsSet().forEach(stream -> ret.putIfAbsent(stream, settings.getExternalStreamPartitions(stream)));
        // check if there is a conflict when input stream partition does not match computation concurrency
        Set<ComputationMetadataMapping> conflicts = topology.metadataList().stream()
                .filter(meta -> checkConflict(meta.name, ret)).collect(Collectors.toSet());
        if (!conflicts.isEmpty()) {
            String msg = "Conflict detected: " + conflicts.stream().map(
                    meta -> "Computation " + meta + " can not apply concurrency of: " + settings.getConcurrency(meta.name))
                    .collect(Collectors.joining(". "));
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
        return ret;
    }

    private boolean checkConflict(String name, Map<String, Integer> ret) {
        return topology.getParents(name).stream().filter(stream -> ret.get(stream) != settings.getConcurrency(name)).count() > 0;
    }

}
