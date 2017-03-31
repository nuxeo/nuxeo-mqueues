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
import org.nuxeo.ecm.platform.importer.mqueues.computation.StreamTailer;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Streams;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Topology;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.StreamFactoryImpl;

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
    public void testManagerConflictSettings() throws Exception {
        Streams streams = new Streams(new StreamFactoryImpl(folder.newFolder().toPath()));
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
        try {
            ComputationManager manager = new ComputationManager(streams, topology, settings);
            manager.stop();
            fail("Conflict not detected");
        } catch (IllegalArgumentException e) {
            // expected in case of conflict
        }

        // fix settings
        settings = new Settings(2);
        ComputationManager manager = new ComputationManager(streams, topology, settings);
        manager.stop();
    }

    @Test
    public void testManagerLineUp() throws Exception {
        final long targetTimestamp = System.currentTimeMillis();
        final long targetWatermark = Watermark.ofTimestamp(targetTimestamp).getValue();
        final int nbRecords = 10;

        Streams streams = new Streams(new StreamFactoryImpl(folder.newFolder().toPath()));

        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Arrays.asList("o1:s1"))
                .addComputation(() -> new ComputationForward("C1", 1, 1), Arrays.asList("i1:s1", "o1:s2"))
                .addComputation(() -> new ComputationForward("C2", 1, 1), Arrays.asList("i1:s2", "o1:s3"))
                .addComputation(() -> new ComputationForward("C3", 1, 1), Arrays.asList("i1:s3", "o1:s4"))
                .addComputation(() -> new ComputationRecordCounter("COUNTER", Duration.ofMillis(100)), Arrays.asList("i1:s4", "o1:output"))
                .build();

        Settings settings = new Settings(1); // one thread for each computation

        ComputationManager manager = new ComputationManager(streams, topology, settings);
        // uncomment to get the plantuml diagram
        // System.out.println(manager.toPlantuml());
        manager.start();
        long start = System.currentTimeMillis();

        while (!manager.isDone(targetTimestamp)) {
            Thread.sleep(30);
            long lowWatermark = manager.getLowWatermark();
            System.out.println("low: " + lowWatermark + " dist: " + (targetWatermark - lowWatermark));
        }
        System.out.println("Elapsed: " + (System.currentTimeMillis() - start));
        // shutdown brutally so there is no more processing in background
        manager.shutdown();

        // read the results
        int result = readCounterFrom(streams, "output");
        assertEquals(nbRecords, result);
    }

    private int readCounterFrom(Streams streams, String output) throws InterruptedException {
        StreamTailer tailer = streams.getStream("output").createTailerForPartition("results", 0);
        int result = 0;
        for (Record record = tailer.read(Duration.ofMillis(1)); record != null; record = tailer.read(Duration.ofMillis(1))) {
            result += Integer.valueOf(record.key);
        }
        return result;
    }

    @Test
    public void testManager() throws Exception {
        final long targetTimestamp = System.currentTimeMillis();
        final long targetWatermark = Watermark.ofTimestamp(targetTimestamp).getValue();
        final int nbRecords = 1000;
        final int concurrent = 17;
        Streams streams = new Streams(new StreamFactoryImpl(folder.newFolder().toPath()));

        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, nbRecords, 5, targetTimestamp), Arrays.asList("o1:s1"))
                .addComputation(() -> new ComputationForward("C1", 1, 2), Arrays.asList("i1:s1", "o1:s2", "o2:s3"))
                .addComputation(() -> new ComputationForward("C2", 2, 1), Arrays.asList("i1:s1", "i2:s4", "o1:s5"))
                .addComputation(() -> new ComputationForward("C3", 1, 2), Arrays.asList("i1:s2", "o1:s5", "o2:s4"))
                //.addComputation(() -> new ComputationForwardSlow("C3", 1, 2, 5), Arrays.asList("i1:s2", "o1:s5", "o2:s4"))
                .addComputation(() -> new ComputationForward("C4", 1, 1), Arrays.asList("i1:s3", "o1:s5"))
                .addComputation(() -> new ComputationRecordCounter("COUNTER", Duration.ofMillis(100)), Arrays.asList("i1:s5", "o1:output"))
                .build();

        Settings settings = new Settings(1) // need to set default to one so output is created with one partition
                .setConcurrency("GENERATOR", concurrent)
                .setConcurrency("C1", concurrent)
                .setConcurrency("C2", concurrent)
                .setConcurrency("C3", concurrent)
                .setConcurrency("C4", concurrent)
                .setConcurrency("COUNTER", 1);

        ComputationManager manager = new ComputationManager(streams, topology, settings);
        // uncomment to get the plantuml diagram
        // System.out.println(manager.toPlantuml());
        long start = System.currentTimeMillis();
        manager.start();

        // no record are processed so far
        long lowWatermark = manager.getLowWatermark();

        assertTrue(manager.drainAndStop(Duration.ofSeconds(100)));
        double elapsed = (double) (System.currentTimeMillis() - start) / 1000.0;
        // read the results
        int result = readCounterFrom(streams, "output");
        assertEquals(2 * settings.getConcurrency("GENERATOR") *nbRecords, result);
        System.out.println(String.format("count: %s in %.2fs, throughput: %.2f records/s", result, elapsed, result / elapsed));
    }


}
