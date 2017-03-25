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
import org.nuxeo.ecm.platform.importer.mqueues.computation.Record;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Stream;
import org.nuxeo.ecm.platform.importer.mqueues.computation.StreamTailer;
import org.nuxeo.ecm.platform.importer.mqueues.computation.Streams;
import org.nuxeo.ecm.platform.importer.mqueues.computation.internals.StreamFactoryImpl;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.EnumSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @since 9.1
 */
public class TestStream {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();


    @Test
    public void testStreams() throws Exception {
        Streams streams = new Streams(new StreamFactoryImpl(folder.newFolder().toPath()));
        Stream stream = streams.getOrCreateStream("foo", 10);
        assertEquals(stream.getName(), "foo");
        assertEquals(stream.getPartitions(), 10);

        Stream stream2 = streams.getOrCreateStream("bar", 5);

        Stream stream3 = streams.getStream("bar");
        assertEquals(stream2, stream3);

        assertNull(streams.getStream("unknown"));
    }


    @Test
    public void testStream() throws Exception {
        Streams streams = new Streams(new StreamFactoryImpl(folder.newFolder().toPath()));
        Stream stream = streams.getOrCreateStream("foo", 1);
        assertEquals(stream.getName(), "foo");

        byte[] data = "some data".getBytes();
        byte[] data2 = "other data".getBytes();

        stream.appendRecord("key1", data);
        stream.appendRecord("key2", data);

        Record r3 = new Record("key3", data2, System.currentTimeMillis(),
                EnumSet.of(Record.Flag.BEGIN, Record.Flag.POISON_PILL));
        stream.appendRecord(r3);

        try (StreamTailer tailer = stream.createTailerForPartition("consumer1", 0)) {
            Record record = tailer.read(Duration.ofMillis(1));
            assertEquals("key1", record.key);
            assertEquals(Arrays.toString(data), Arrays.toString(record.data));
            assertFalse(record.flags.contains(Record.Flag.DEFAULT));
            assertFalse(record.flags.contains(Record.Flag.BEGIN));
            assertFalse(record.flags.contains(Record.Flag.POISON_PILL));

            record = tailer.read(Duration.ofMillis(1));
            assertEquals("key2", record.key);
            assertEquals(Arrays.toString(data), Arrays.toString(record.data));

            record = tailer.read(Duration.ofMillis(1));
            assertEquals("key3", record.key);
            assertEquals(Arrays.toString(data2), Arrays.toString(record.data));
            assertTrue(record.flags.contains(Record.Flag.BEGIN));
            assertTrue(record.flags.contains(Record.Flag.POISON_PILL));

            record = tailer.read(Duration.ofMillis(1));
            assertNull(record);

        }
    }

    @Test
    public void testNullRecord() throws Exception {
        Streams streams = new Streams(new StreamFactoryImpl(folder.newFolder().toPath()));
        Stream stream = streams.getOrCreateStream("foo", 1);

        stream.appendRecord("foo", null);
        try (StreamTailer tailer = stream.createTailerForPartition("consumer1", 0)) {
            Record record = tailer.read(Duration.ofSeconds(1));
            assertNotNull(record);
            assertEquals("foo", record.key);
            assertEquals(null, record.data);
            assertTrue(record.watermark > 0);
            assertTrue(record.flags.isEmpty());
        }

        try {
            // key can not be null
            stream.appendRecord(null, null);
            fail("exception not thrown");
        } catch (NullPointerException e) {
            // expected
        }

    }



}
