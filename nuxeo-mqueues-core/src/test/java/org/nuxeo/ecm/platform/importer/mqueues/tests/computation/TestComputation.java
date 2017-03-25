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
import org.nuxeo.ecm.platform.importer.mqueues.computation.Computation;
import org.nuxeo.ecm.platform.importer.mqueues.computation.ComputationMetadataMapping;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Watermark;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.ComputationContextImpl;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.WatermarkInterval;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @since 9.1
 */
public class TestComputation {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Test
    public void testForwardComputation() throws Exception {
        // create a computation with 2 inputs streams and 4 output streams
        Computation comp = new ComputationForward("foo", 2, 4);

        // check expected metadata
        assertEquals("foo", comp.metadata().name);
        assertEquals(new HashSet(Arrays.asList("i1", "i2")), comp.metadata().istreams);
        assertEquals(new HashSet(Arrays.asList("o1", "o2", "o3", "o4")), comp.metadata().ostreams);

        // create a context
        ComputationContextImpl context = new ComputationContextImpl(new ComputationMetadataMapping(comp.metadata(),
                Collections.emptyMap()));

        // init the computation
        comp.init(context);

        // there is no timer
        assertEquals(0, context.getTimers().size());

        // ask to process a record
        comp.processRecord(context, "i1", Record.of("foo", "bar".getBytes()));

        // the record is forwarded to all output stream
        assertEquals(1, context.getRecords("o1").size());
        assertEquals(1, context.getRecords("o2").size());
        assertEquals(1, context.getRecords("o3").size());
        assertEquals(1, context.getRecords("o4").size());

        assertEquals("foo", context.getRecords("o2").get(0).key);
        assertEquals("bar", new String(context.getRecords("o1").get(0).data, StandardCharsets.UTF_8));

        comp.destroy();
    }


    @Test
    public void testSource() throws Exception {
        int nbRecordsToGenerate = 7;
        int batchSize = 3;
        int outputStreams = 2;

        Computation comp = new ComputationSource("foo", outputStreams, nbRecordsToGenerate, batchSize);
        assertEquals("foo", comp.metadata().name);
        assertEquals(new HashSet(Arrays.asList("i1")), comp.metadata().istreams);
        assertEquals(new HashSet(Arrays.asList("o1", "o2")), comp.metadata().ostreams);

        ComputationContextImpl context = new ComputationContextImpl(new ComputationMetadataMapping(comp.metadata(),
                Collections.emptyMap()));
        comp.init(context);

        // there is no timer
        assertEquals(0, context.getTimers().size());

        // send a record so it starts to generates records
        long t0 = System.currentTimeMillis();
        long w0 = Watermark.ofTimestamp(t0).getValue();
        comp.processRecord(context, "i1", new Record("start", null, w0, null));

        // there is now a timer
        assertEquals(1, context.getTimers().size());
        String timerKey = (String) context.getTimers().keySet().toArray()[0];
        // execute the timer 3 times
        comp.processTimer(context, timerKey, 0);
        comp.processTimer(context, timerKey, 0);
        comp.processTimer(context, timerKey, 0);

        // this produces 10 record on each output stream
        assertEquals(nbRecordsToGenerate, context.getRecords("o1").size());
        assertEquals(nbRecordsToGenerate, context.getRecords("o2").size());
        // System.out.println(t0);
        // context.getRecords("o1").stream().forEach(record -> System.out.println(Watermark.of(record.watermark)));
        // end up with the expected timestamp
        assertEquals(t0, Watermark.ofValue(context.getSourceLowWatermark()).getTimestamp());

        comp.destroy();
    }

    @Test
    public void testSink() throws Exception {
        Computation comp = new ComputationSinkRecordCounter("foo");
        assertEquals("foo", comp.metadata().name);
        assertEquals(new HashSet(Arrays.asList("i1", "i2")), comp.metadata().istreams);
        assertEquals(new HashSet(Arrays.asList("o1")), comp.metadata().ostreams);

        ComputationContextImpl context = new ComputationContextImpl(new ComputationMetadataMapping(comp.metadata(),
                Collections.emptyMap()));
        comp.init(context);

        for (int i=0; i<42; i++) {
            comp.processRecord(context, "i1", Record.of("foo", "bar".getBytes()));
        }
        assertEquals(0, context.getRecords("o1").size());

        // ask for the total by sending any record to i2
        comp.processRecord(context, "i2", Record.of("foo", "bar".getBytes()));
        assertEquals(1, context.getRecords("o1").size());

        // the key contains the total
        assertEquals("42", context.getRecords("o1").get(0).key);

        // the counter is reset
        comp.processRecord(context, "i2", Record.of("foo", "bar".getBytes()));
        assertEquals(2, context.getRecords("o1").size());
        assertEquals("0", context.getRecords("o1").get(1).key);

    }


}
