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
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationManager;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationMetadataMapping;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Settings;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Topology;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;

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
public class MQComputationManager implements ComputationManager {
    private static final Log log = LogFactory.getLog(MQComputationManager.class);
    private final MQManager<Record> mqManager;
    private final Topology topology;
    private final Settings settings;
    private final List<MQComputationPool> pools;

    public MQComputationManager(MQManager<Record> mqManager, Topology topology, Settings settings) {
        this.mqManager = mqManager;
        this.topology = topology;
        this.settings = settings;
        initStreams();
        this.pools = initPools();
        Objects.requireNonNull(pools);
    }

    @Override
    public void start() {
        log.debug("Starting ...");
        pools.forEach(MQComputationPool::start);
    }

    @Override
    public boolean stop(Duration timeout) {
        log.debug("Starting ...");
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
        log.debug("Shutdown ...");
        pools.parallelStream().forEach(MQComputationPool::shutdown);
        log.debug("Shutdown done");
    }


    @Override
    public long getLowWatermark() {
        Map<String, Long> watermarks = new HashMap<>(pools.size());
        Set<String> roots = topology.getRoots();
        Map<String, Long> watermarkTrees = new HashMap<>(roots.size());
        // compute low watermark for each tree of computation
        pools.forEach(pool -> watermarks.put(pool.getComputationName(), pool.getLowWatermark()));
        for (String root : roots) {
            watermarkTrees.put(root, topology.getDescendantComputationNames(root).stream().map(watermarks::get)
                    .min(Comparator.naturalOrder()).orElse(0L));
        }
        // return the minimum wm for all trees that are not 0
        long ret = watermarkTrees.values().stream()
                .filter(wm -> wm > 1).min(Comparator.naturalOrder()).orElse(0L);
        if (log.isTraceEnabled()) {
            log.trace("lowWatermark: " + ret);
            watermarkTrees.forEach((k, v) -> log.trace("tree " + k + ": " + v));
            // topology.metadataList().forEach(meta -> System.out.println("  low " + meta.name + " : \t" + getLowWatermark(meta.name)));
        }
        return ret;
    }

    @Override
    public long getLowWatermark(String computationName) {
        Objects.nonNull(computationName);
        // the low wm for a computation is the minimum watermark for all its ancestors
        Map<String, Long> watermarks = new HashMap<>(pools.size());
        pools.forEach(pool -> watermarks.put(pool.getComputationName(), pool.getLowWatermark()));
        long ret = topology.getAncestorComputationNames(computationName).stream().map(watermarks::get)
                .min(Comparator.naturalOrder()).orElse(0L);
        ret = Math.min(ret, watermarks.get(computationName));
        return ret;
    }

    @Override
    public boolean isDone(long timestamp) {
        return Watermark.ofValue(getLowWatermark()).isDone(timestamp);
    }

    private List<MQComputationPool> initPools() {
        log.debug("Initializing pools");
        return topology.metadataList().stream()
                .map(meta -> new MQComputationPool(topology.getSupplier(meta.name), meta,
                        settings.getConcurrency(meta.name), mqManager))
                .collect(Collectors.toList());
    }

    private void initStreams() {
        log.debug("Initializing streams");
        Map<String, Integer> streamPartitions = computePartitions();
        topology.streamsSet().forEach(name -> mqManager.openOrCreate(name, streamPartitions.get(name)));
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
