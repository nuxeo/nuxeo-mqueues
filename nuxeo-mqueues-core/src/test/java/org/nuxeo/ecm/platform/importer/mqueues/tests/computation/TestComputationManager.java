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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationManager;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Settings;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Topology;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.ComputationManagerImpl;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.mq.StreamsMQ;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.StreamTailer;
import org.nuxeo.ecm.platform.importer.mqueues.computation.spi.Streams;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @since 9.1
 */
public class TestComputationManager {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Test
    public void testConflictSettings() throws Exception {
        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("C1"), Arrays.asList("o1:s1"))
                .addComputation(() -> new ComputationForward("C2", 1, 0), Arrays.asList("i1:s1"))
                .addComputation(() -> new ComputationForward("C3", 1, 0), Arrays.asList("i1:s1"))
                .build();
        // Concurrency must be the same for all computations that share an input stream
        // Here C2 and C3 read from s1 with different concurrency
        Settings settings = new Settings(4)
                .setConcurrency("C1", 1)
                .setConcurrency("C2", 2);

        try (Streams streams = new StreamsMQ(folder.newFolder().toPath())) {
            try {
                ComputationManager manager = new ComputationManagerImpl(streams, topology, settings);
                manager.stop();
                fail("Conflict not detected");
            } catch (IllegalArgumentException e) {
                // expected in case of conflict
            }
            // fix settings
            settings = new Settings(2);
            ComputationManager manager = new ComputationManagerImpl(streams, topology, settings);
            manager.stop();
        }
    }

    public void testSimpleTopo(int nbRecords, int concurrency) throws Exception {
        final long targetTimestamp = System.currentTimeMillis();
        final long targetWatermark = Watermark.ofTimestamp(targetTimestamp).getValue();
        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Arrays.asList("o1:s1"))
                .addComputation(() -> new ComputationForward("C1", 1, 1), Arrays.asList("i1:s1", "o1:s2"))
                .addComputation(() -> new ComputationForward("C2", 1, 1), Arrays.asList("i1:s2", "o1:s3"))
                .addComputation(() -> new ComputationForward("C3", 1, 1), Arrays.asList("i1:s3", "o1:s4"))
                .addComputation(() -> new ComputationRecordCounter("COUNTER", Duration.ofMillis(100)), Arrays.asList("i1:s4", "o1:output"))
                .build();
        // one thread for each computation
        Settings settings = new Settings(concurrency);
        // uncomment to get the plantuml diagram
        System.out.println(topology.toPlantuml(settings));
        try (Streams streams = new StreamsMQ(folder.newFolder().toPath())) {
            ComputationManager manager = new ComputationManagerImpl(streams, topology, settings);
            manager.start();
            long start = System.currentTimeMillis();
            // this check works only if there is only one record with the target timestamp
            // so using concurrency > 1 will fail
            while (!manager.isDone(targetTimestamp)) {
                Thread.sleep(30);
                long lowWatermark = manager.getLowWatermark();
                System.out.println("low: " + lowWatermark + " dist: " + (targetWatermark - lowWatermark));
            }
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            // shutdown brutally so there is no more processing in background
            manager.shutdown();

            // read the results
            int result = readCounterFrom(streams, "output");
            assertEquals(nbRecords * concurrency, result);
            System.out.println(String.format("topo: simple, concurrency: %d, records: %s, took: %.2fs, throughput: %.2f records/s",
                    concurrency, result, elapsed, result / elapsed));
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

    public void testComplexTopo(int nbRecords, int concurrency) throws Exception {
        final long targetTimestamp = System.currentTimeMillis();
        final long targetWatermark = Watermark.ofTimestamp(targetTimestamp).getValue();
        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Arrays.asList("o1:s1"))
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
        try (Streams streams = new StreamsMQ(folder.newFolder().toPath())) {
            ComputationManager manager = new ComputationManagerImpl(streams, topology, settings);
            long start = System.currentTimeMillis();
            manager.start();

            // no record are processed so far
            long lowWatermark = manager.getLowWatermark();

            assertTrue(manager.drainAndStop(Duration.ofSeconds(100)));
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            // read the results
            int result = readCounterFrom(streams, "output");
            assertEquals(2 * settings.getConcurrency("GENERATOR") * nbRecords, result);
            System.out.println(String.format("topo: complex, concurrency: %d, records: %s, took: %.2fs, throughput: %.2f records/s",
                    concurrency, result, elapsed, result / elapsed));
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
        final Path base = folder.newFolder().toPath();
        Topology topology1 = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Arrays.asList("o1:s1"))
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
                .setConcurrency("COUNTER", 1);
        // uncomment to get the plantuml diagram
        // System.out.println(topology.toPlantuml(settings));
        // 1. run generators
        try (Streams streams = new StreamsMQ(base)) {
            ComputationManager manager = new ComputationManagerImpl(streams, topology1, settings1);
            long start = System.currentTimeMillis();
            manager.start();
            // no record are processed so far
            assertTrue(manager.drainAndStop(Duration.ofSeconds(100)));
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            long total = settings1.getConcurrency("GENERATOR") * nbRecords;
            System.out.println(String.format("generated :%s in %.2fs, throughput: %.2f records/s", total, elapsed, total / elapsed));
        }
        int result = 0;
        // run and abort
        for (int i = 0; i < 10; i++) {
            try (Streams streams = new StreamsMQ(base)) {
                ComputationManager manager = new ComputationManagerImpl(streams, topology2, settings2);
                long start = System.currentTimeMillis();
                System.out.println("RESUME ");
                manager.start();
                Thread.sleep(110);
                System.out.println("KILL");
                manager.shutdown();
                long processed = readCounterFrom(streams, "output");
                result += processed;
                System.out.println("processed: " + processed + " total: " + result);
            }
        }

        // 2. run the rest
        try (Streams streams = new StreamsMQ(base)) {
            ComputationManager manager = new ComputationManagerImpl(streams, topology2, settings2);
            long start = System.currentTimeMillis();
            manager.start();
            assertTrue(manager.drainAndStop(Duration.ofSeconds(100)));
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            // read the results
            long processed = readCounterFrom(streams, "output");
            result += processed;
            assertEquals(2 * settings1.getConcurrency("GENERATOR") * nbRecords, result);
            System.out.println(String.format("count: %s in %.2fs, throughput: %.2f records/s", result, elapsed, result / elapsed));
        }

    }

    @Test
    public void testDrainSource() throws Exception {
        final long targetTimestamp = System.currentTimeMillis();
        final int nbRecords = 1;
        final int concurrent = 10;
        final Path base = folder.newFolder().toPath();
        Topology topology1 = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Arrays.asList("o1:s1"))
                .build();

        Settings settings1 = new Settings(concurrent);

        try (Streams streams = new StreamsMQ(base)) {
            ComputationManager manager = new ComputationManagerImpl(streams, topology1, settings1);
            long start = System.currentTimeMillis();
            manager.start();
            // no record are processed so far
            assertTrue(manager.drainAndStop(Duration.ofSeconds(100)));
            double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
            int result = countRecordIn(streams, "s1");
            assertEquals(settings1.getConcurrency("GENERATOR") * nbRecords, result);
            System.out.println(String.format("count: %s in %.2fs, throughput: %.2f records/s", result, elapsed, result / elapsed));
        }

    }

    private int readCounterFrom(Streams streams, String stream) throws InterruptedException {
        int partitions = streams.getStream(stream).getPartitions();
        int ret = 0;
        for (int i = 0; i < partitions; i++) {
            ret += readCounterFromPartion(streams, stream, i);
        }
        return ret;
    }

    private int readCounterFromPartion(Streams streams, String stream, int partition) throws InterruptedException {
        StreamTailer tailer = streams.getStream(stream).createTailerForPartition("results", partition);
        int result = 0;
        for (Record record = tailer.read(Duration.ofMillis(1)); record != null; record = tailer.read(Duration.ofMillis(1))) {
            result += Integer.valueOf(record.key);
        }
        tailer.commit();
        return result;
    }


    private int countRecordIn(Streams streams, String stream) throws Exception {
        int ret = 0;
        for (int i = 0; i < streams.getStream(stream).getPartitions(); i++) {
            ret += countRecordInPartition(streams, stream, i);
        }
        return ret;
    }

    private int countRecordInPartition(Streams streams, String stream, int partition) throws Exception {
        try (StreamTailer tailer = streams.getStream(stream).createTailerForPartition("results", partition)) {
            int result = 0;
            for (Record record = tailer.read(Duration.ofMillis(1)); record != null; record = tailer.read(Duration.ofMillis(1))) {
                result += 1;
            }
            return result;
        }
    }

}
