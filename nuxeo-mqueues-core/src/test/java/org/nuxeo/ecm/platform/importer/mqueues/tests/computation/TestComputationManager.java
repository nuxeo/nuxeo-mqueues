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
import static org.junit.Assert.assertNotNull;
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
                .addComputation(() -> new ComputationSource("C1", 1), Arrays.asList("o1:s1"))
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
        Streams streams = new Streams(new StreamFactoryImpl(folder.newFolder().toPath()));

        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, 10, 5), Arrays.asList("i1:input", "o1:s1"))
                .addComputation(() -> new ComputationForward("C1", 1, 1), Arrays.asList("i1:s1", "o1:s2"))
                .addComputation(() -> new ComputationForward("C2", 1, 1), Arrays.asList("i1:s2", "o1:s3"))
                .addComputation(() -> new ComputationForward("C3", 1, 1), Arrays.asList("i1:s3", "o1:s4"))
                .addComputation(() -> new ComputationSinkRecordCounter("COUNTER"), Arrays.asList("i1:s4", "i2:command", "o1:output"))
                .build();

        Settings settings = new Settings(1); // one thread for each computation

        ComputationManager manager = new ComputationManager(streams, topology, settings);
        manager.start();

        // no record are processed so far
        long lowWatermark = manager.getLowWatermark();
        assertEquals(0, lowWatermark);

        // send a signal to start record generation
        long start = System.currentTimeMillis();
        long targetWatermark = Watermark.ofTimestamp(start).getValue();
        System.out.println("target Watermark: " + Watermark.ofTimestamp(start));
        streams.getStream("input").appendRecord(new Record("generate records", null, targetWatermark, null));

        while (!manager.isDone(start)) {
            Thread.sleep(30);
            lowWatermark = manager.getLowWatermark();
            System.out.println("low: " + lowWatermark + " dist: " + (targetWatermark - lowWatermark));
        }

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Elapsed: " + elapsed);

        // ask for the total
        streams.getStream("command").appendRecord("the bill please", null);
        StreamTailer tailer = streams.getStream("output").createTailerForPartition("results", 0);
        Record result = tailer.read(Duration.ofSeconds(1));
        assertNotNull(result);
        assertEquals("10", result.key);

        manager.stop();

    }

    @Test
    public void testManager() throws Exception {
        Streams streams = new Streams(new StreamFactoryImpl(folder.newFolder().toPath()));

        Topology topology = Topology.builder()
                .addComputation(() -> new ComputationSource("GENERATOR", 1, 10, 5), Arrays.asList("i1:input", "o1:s1"))
                .addComputation(() -> new ComputationForward("C2", 1, 2), Arrays.asList("i1:s1", "o1:s2", "o2:s3"))
                .addComputation(() -> new ComputationForward("C3", 2, 1), Arrays.asList("i1:s1", "i2:s4", "o1:s5"))
                .addComputation(() -> new ComputationForwardSlow("C4", 1, 2, 5), Arrays.asList("i1:s2", "o1:s5", "o2:s4"))
                .addComputation(() -> new ComputationForward("C5", 1, 1), Arrays.asList("i1:s3", "o1:s5"))
                .addComputation(() -> new ComputationSinkRecordCounter("COUNTER"), Arrays.asList("i1:s5", "i2:command", "o1:output"))
                .build();

        Settings settings = new Settings(1) // need to set default to one so output is created with one partition
                .setConcurrency("GENERATOR", 1)
                .setConcurrency("C4", 2)
                .setConcurrency("C5", 3)
                .setConcurrency("COUNTER", 1);

        ComputationManager manager = new ComputationManager(streams, topology, settings);
        manager.start();

        // no record are processed so far
        long lowWatermark = manager.getLowWatermark();
        assertEquals(0, lowWatermark);

        // send a signal to start record generation
        long start = System.currentTimeMillis();
        long targetWatermark = Watermark.ofTimestamp(start).getValue();
        System.out.println("target Watermark: " + Watermark.ofTimestamp(start));
        streams.getStream("input").appendRecord(new Record("generate records", null, targetWatermark, null));

        while (!manager.isDone(start)) {
            Thread.sleep(30);
            lowWatermark = manager.getLowWatermark();
            if (lowWatermark > 0) {
                System.out.println("low : " + lowWatermark + " dist: " + (targetWatermark - lowWatermark
                ));
            }
        }
        System.out.println("End low wm: " + lowWatermark + " greater: " + (lowWatermark - targetWatermark));

        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Elapsed: " + elapsed);

        streams.getStream("command").appendRecord("the bill please", null);

        // read the counter
        StreamTailer tailer = streams.getStream("output").createTailerForPartition("results", 0);

        Record result = tailer.read(Duration.ofSeconds(1));
        assertNotNull(result);
        System.out.println("Out low " + manager.getLowWatermark());
        ;

        // not possible because message are duplicated
        // assertEquals("40", result.key);
        System.out.println("Received count: " + result.key);
        lowWatermark = manager.getLowWatermark();

        manager.stop();

    }


}
