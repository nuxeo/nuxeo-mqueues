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
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.kafka.KafkaUtils;

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
    private final MQManager<Record> manager;
    private final Topology topology;
    private final Settings settings;
    private final List<MQComputationPool> pools;

    public MQComputationManager(MQManager<Record> manager, Topology topology, Settings settings) {
        this.manager = manager;
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
    public boolean waitForAssignments(Duration timeout) throws InterruptedException {
        for (MQComputationPool pool : pools) {
            if (! pool.waitForAssignments(timeout)) {
                return false;
            }
        }
        return true;
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
                .map(meta -> new MQComputationPool(topology.getSupplier(meta.name()), meta,
                        getDefaultAssignments(meta), manager))
                .collect(Collectors.toList());
    }

    private List<List<MQPartition>> getDefaultAssignments(ComputationMetadataMapping meta) {
        int threads = settings.getConcurrency(meta.name());
        Map<String, Integer> streams = new HashMap<>();
        meta.inputStreams().forEach(streamName -> streams.put(streamName, settings.getPartitions(streamName)));
        return KafkaUtils.roundRobinAssignments(threads, streams);
    }

    private void initStreams() {
        log.debug("Initializing streams");
        topology.streamsSet().forEach(streamName -> manager.createIfNotExists(streamName, settings.getPartitions(streamName)));
    }

}
