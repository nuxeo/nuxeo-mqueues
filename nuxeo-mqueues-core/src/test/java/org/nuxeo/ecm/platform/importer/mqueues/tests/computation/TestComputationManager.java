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
package org.nuxeo.ecm.platform.importer.mqueues.tests.computation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationManager;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Settings;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Topology;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @since 9.1
 */
public abstract class TestComputationManager {
    private static final Log log = LogFactory.getLog(TestComputationManager.class);

    public abstract MQManager<Record> getStreams() throws Exception;

    public abstract MQManager<Record> getSameStreams() throws Exception;

    public abstract ComputationManager getManager(MQManager<Record> mqManager, Topology topology, Settings settings);

    @Test
    public void testConflictSettings() throws Exception {
        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("C1"), Collections.singletonList("o1:s1"))
                .addComputation(() -> new ComputationForward("C2", 1, 0), Collections.singletonList("i1:s1"))
                .addComputation(() -> new ComputationForward("C3", 1, 0), Collections.singletonList("i1:s1"))
                .build();
        // Concurrency must be the same for all computations that share an input stream
        // Here C2 and C3 read from s1 with different concurrency
        Settings settings = new Settings(4)
                .setConcurrency("C1", 1)
                .setConcurrency("C2", 2);

        try (MQManager<Record> streams = getStreams()) {
            try {
                ComputationManager manager = getManager(streams, topology, settings);
                manager.stop();
                fail("Conflict not detected");
            } catch (IllegalArgumentException e) {
                // expected in case of conflict
            }
            // fix settings
            settings = new Settings(2);
            ComputationManager manager = getManager(streams, topology, settings);
            manager.stop();
        }
    }


    public void testSimpleTopo(int nbRecords, int concurrency) throws Exception {
        final long targetTimestamp = System.currentTimeMillis();
        final long targetWatermark = Watermark.ofTimestamp(targetTimestamp).getValue();
        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Collections.singletonList("o1:s1"))
                .addComputation(() -> new ComputationForward("C1", 1, 1), Arrays.asList("i1:s1", "o1:s2"))
                .addComputation(() -> new ComputationForward("C2", 1, 1), Arrays.asList("i1:s2", "o1:s3"))
                .addComputation(() -> new ComputationForward("C3", 1, 1), Arrays.asList("i1:s3", "o1:s4"))
                .addComputation(() -> new ComputationRecordCounter("COUNTER", Duration.ofMillis(100)), Arrays.asList("i1:s4", "o1:output"))
                .build();
        // one thread for each computation
        Settings settings = new Settings(concurrency).setConcurrency("GENERATOR", 1);
        // uncomment to get the plantuml diagram
        // System.out.println(topology.toPlantuml(settings));
        try (MQManager<Record> streams = getStreams()) {
            ComputationManager manager = getManager(streams, topology, settings);
            manager.start();
            long start = System.currentTimeMillis();
            // this check works only if there is only one record with the target timestamp
            // so using concurrency > 1 will fail
            while (!manager.isDone(targetTimestamp)) {
                Thread.sleep(30);
                long lowWatermark = manager.getLowWatermark();
                log.info("low: " + lowWatermark + " dist: " + (targetWatermark - lowWatermark));
            }
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            // shutdown brutally so there is no more processing in background
            manager.shutdown();

            // read the results
            int result = readCounterFrom(streams, "output");
            int expected = nbRecords * settings.getConcurrency("GENERATOR");
            if (result != expected) {
                manager = getManager(streams, topology, settings);
                manager.start();
                int waiter = 200;
                log.warn("FAILURE DEBUG TRACE ========================");
                do {
                    waiter -= 10;
                    Thread.sleep(10);
                    long lowWatermark = manager.getLowWatermark();
                    log.warn("low: " + lowWatermark + " dist: " + (targetWatermark - lowWatermark));
                } while (waiter > 0);
                manager.shutdown();
            }
            log.info(String.format("topo: simple, concurrency: %d, records: %s, took: %.2fs, throughput: %.2f records/s",
                    concurrency, result, elapsed, result / elapsed));
            assertEquals(expected, result);
        }
    }

    @Test
    public void testSimpleTopoOneRecordOneThread() throws Exception {
        testSimpleTopo(1, 1);
    }

    @Test
    public void testSimpleTopoFewRecordsOneThread() throws Exception {
        testSimpleTopo(17, 1);
    }

    @Test
    public void testSimpleTopoManyRecordsOneThread() throws Exception {
        testSimpleTopo(1003, 1);
    }

    // @Test
    public void testSimpleTopoManyRecordsManyThread() throws Exception {
        // because of the concurrency record arrive in disorder in the final counter
        // if the last record is processed by the final counter
        // the global watermark is reached but this does not means that there
        // no pending records on the counter stream, so this test can fail
        // for this kind of test we should test like with testComplexTopo
        // using drainAndStop
        testSimpleTopo(205, 10);
    }


    public void testComplexTopo(int nbRecords, int concurrency) throws Exception {
        final long targetTimestamp = System.currentTimeMillis();
        final long targetWatermark = Watermark.ofTimestamp(targetTimestamp).getValue();
        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Collections.singletonList("o1:s1"))
                .addComputation(() -> new ComputationForward("C1", 1, 2), Arrays.asList("i1:s1", "o1:s2", "o2:s3"))
                .addComputation(() -> new ComputationForward("C2", 2, 1), Arrays.asList("i1:s1", "i2:s4", "o1:s5"))
                .addComputation(() -> new ComputationForward("C3", 1, 2), Arrays.asList("i1:s2", "o1:s5", "o2:s4"))
                //.addComputation(() -> new ComputationForwardSlow("C3", 1, 2, 5), Arrays.asList("i1:s2", "o1:s5", "o2:s4"))
                .addComputation(() -> new ComputationForward("C4", 1, 1), Arrays.asList("i1:s3", "o1:s5"))
                .addComputation(() -> new ComputationRecordCounter("COUNTER", Duration.ofMillis(100)), Arrays.asList("i1:s5", "o1:output"))
                .build();

        Settings settings = new Settings(concurrency).setExternalStreamPartitions("output", 1);
        // uncomment to get the plantuml diagram
        // System.out.println(topology.toPlantuml(settings));
        try (MQManager<Record> streams = getStreams()) {
            ComputationManager manager = getManager(streams, topology, settings);
            long start = System.currentTimeMillis();
            manager.start();

            // no record are processed so far
            long lowWatermark = manager.getLowWatermark();

            assertTrue(manager.drainAndStop(Duration.ofSeconds(100)));
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            // read the results
            int result = readCounterFrom(streams, "output");
            log.info(String.format("topo: complex, concurrency: %d, records: %s, took: %.2fs, throughput: %.2f records/s",
                    concurrency, result, elapsed, result / elapsed));
            assertEquals(2 * settings.getConcurrency("GENERATOR") * nbRecords, result);
        }
    }

    @Test
    public void testComplexTopoOneRecordOneThread() throws Exception {
        testComplexTopo(1, 1);
    }

    @Test
    public void testComplexTopoFewRecordsOneThread() throws Exception {
        testComplexTopo(17, 1);
    }

    @Test
    public void testComplexTopoManyRecordsOneThread() throws Exception {
        testComplexTopo(1003, 1);
    }

    @Test
    public void testComplexTopoOneRecord() throws Exception {
        testComplexTopo(1, 8);
    }

    @Test
    public void testComplexTopoFewRecords() throws Exception {
        testComplexTopo(17, 8);
    }

    @Test
    public void testComplexTopoManyRecords() throws Exception {
        testComplexTopo(1003, 16);
    }

    @Test
    public void testStopAndResume() throws Exception {
        final long targetTimestamp = System.currentTimeMillis();
        final long targetWatermark = Watermark.ofTimestamp(targetTimestamp).getValue();
        final int nbRecords = 1001;
        final int concurrent = 32;
        Topology topology1 = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Collections.singletonList("o1:s1"))
                .build();

        Settings settings1 = new Settings(concurrent);

        Topology topology2 = Topology.builder()
                .addComputation(() -> new ComputationForward("C1", 1, 2), Arrays.asList("i1:s1", "o1:s2", "o2:s3"))
                .addComputation(() -> new ComputationForward("C2", 2, 1), Arrays.asList("i1:s1", "i2:s4", "o1:s5"))
                .addComputation(() -> new ComputationForward("C3", 1, 2), Arrays.asList("i1:s2", "o1:s5", "o2:s4"))
                .addComputation(() -> new ComputationForward("C4", 1, 1), Arrays.asList("i1:s3", "o1:s5"))
                .addComputation(() -> new ComputationRecordCounter("COUNTER", Duration.ofMillis(100)), Arrays.asList("i1:s5", "o1:output"))
                .build();

        Settings settings2 = new Settings(concurrent).setExternalStreamPartitions("output", 1)
                .setConcurrency("COUNTER", concurrent);
        // uncomment to get the plantuml diagram
        // System.out.println(topology.toPlantuml(settings));

        // 1. run generators
        try (MQManager<Record> streams = getStreams()) {
            ComputationManager manager = getManager(streams, topology1, settings1);
            long start = System.currentTimeMillis();
            manager.start();
            // no record are processed so far
            assertTrue(manager.drainAndStop(Duration.ofSeconds(100)));
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            long total = settings1.getConcurrency("GENERATOR") * nbRecords;
            log.info(String.format("generated :%s in %.2fs, throughput: %.2f records/s", total, elapsed, total / elapsed));
        }
        int result = 0;
        // 2. resume and kill loop
        for (int i = 0; i < 10; i++) {
            try (MQManager<Record> streams = getSameStreams()) {
                ComputationManager manager = getManager(streams, topology2, settings2);
                long start = System.currentTimeMillis();
                log.info("RESUME computations");
                manager.start();
                Thread.sleep(50 + i * 10);
                log.info("KILL computations pool");
                manager.shutdown();
                long processed = readCounterFrom(streams, "output");
                result += processed;
                log.info("processed: " + processed + " total: " + result);
            }
        }
        // 3. run the rest
        try (MQManager<Record> streams = getSameStreams()) {
            ComputationManager manager = getManager(streams, topology2, settings2);
            long start = System.currentTimeMillis();
            manager.start();
            assertTrue(manager.drainAndStop(Duration.ofSeconds(100)));
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            // read the results
            long processed = readCounterFrom(streams, "output");
            result += processed;
            // the number of results can be bigger than expected, in the case of checkpoint failure
            // some records can be reprocessed (duplicate), this is a delivery at least one, not exactly one.
            long expected = 2 * settings1.getConcurrency("GENERATOR") * nbRecords;
            log.error(String.format("count: %s, expected: %s, in %.2fs, throughput: %.2f records/s",
                    result, expected, elapsed, result / elapsed));
            assertTrue(expected <= result);
        }

    }

    @Test
    public void testDrainSource() throws Exception {
        final long targetTimestamp = System.currentTimeMillis();
        final int nbRecords = 1;
        final int concurrent = 10;
        Topology topology1 = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Collections.singletonList("o1:s1"))
                .build();

        Settings settings1 = new Settings(concurrent);

        try (MQManager<Record> streams = getStreams()) {
            ComputationManager manager = getManager(streams, topology1, settings1);
            long start = System.currentTimeMillis();
            manager.start();
            // no record are processed so far
            assertTrue(manager.drainAndStop(Duration.ofSeconds(100)));
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            int result = countRecordIn(streams, "s1");
            log.info(String.format("count: %s in %.2fs, throughput: %.2f records/s", result, elapsed, result / elapsed));
            assertEquals(settings1.getConcurrency("GENERATOR") * nbRecords, result);
        }

    }

    private int readCounterFrom(MQManager<Record> manager, String stream) throws InterruptedException {
        int partitions = manager.getAppender(stream).size();
        int ret = 0;
        for (int i = 0; i < partitions; i++) {
            ret += readCounterFromPartition(manager, stream, i);
        }
        return ret;
    }

    private int readCounterFromPartition(MQManager<Record> manager, String stream, int partition) throws InterruptedException {
        MQTailer<Record> tailer = manager.createTailer(MQPartition.of(stream, partition), "results");
        int result = 0;
        for (Record record = tailer.read(Duration.ofMillis(1000)); record != null; record = tailer.read(Duration.ofMillis(1))) {
            result += Integer.valueOf(record.key);
        }
        tailer.commit();
        return result;
    }


    private int countRecordIn(MQManager<Record> manager, String stream) throws Exception {
        int ret = 0;
        for (int i = 0; i < manager.getAppender(stream).size(); i++) {
            ret += countRecordInPartition(manager, stream, i);
        }
        return ret;
    }

    private int countRecordInPartition(MQManager<Record> manager, String stream, int partition) throws Exception {
        try (MQTailer<Record> tailer = manager.createTailer(MQPartition.of(stream, partition), "results")) {
            int result = 0;
            for (Record record = tailer.read(Duration.ofMillis(100)); record != null; record = tailer.read(Duration.ofMillis(1))) {
                result += 1;
            }
            return result;
        }
    }

}
