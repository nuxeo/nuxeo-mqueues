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

import org.junit.Test;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Record;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Stream;
import org.nuxeo.ecm.platform.importer.mqueues.streams.StreamTailer;
import org.nuxeo.ecm.platform.importer.mqueues.streams.Streams;

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
public abstract class TestStream {

    public abstract Streams getStreams() throws Exception;

    public abstract Streams getSameStreams() throws Exception;

    @Test
    public void testStreams() throws Exception {
        Streams streams = getStreams();
        String name1 = "foo";
        Stream stream = streams.getOrCreateStream(name1, 10);
        assertEquals(name1, stream.getName());
        assertEquals(10, stream.getPartitions());

        String name2 = "bar";
        Stream stream2 = streams.getOrCreateStream(name2, 5);
        Stream stream3 = streams.getStream(name2);
        assertEquals(stream2, stream3);
        stream3 = streams.getOrCreateStream(name2, 5);
        assertEquals(stream2, stream3);

        assertNull(streams.getStream("unknown"));
    }


    @Test
    public void testStream() throws Exception {
        Streams streams = getStreams();
        String name = "foo";
        Stream stream = streams.getOrCreateStream(name, 1);
        assertEquals(name, stream.getName());

        byte[] data = "some data".getBytes();
        byte[] data2 = "other data".getBytes();

        stream.appendRecord("key1", data);
        stream.appendRecord("key2", data);

        Record r3 = new Record("key3", data2, System.currentTimeMillis(),
                EnumSet.of(Record.Flag.BEGIN, Record.Flag.POISON_PILL));
        stream.appendRecord(r3);

        try (StreamTailer tailer = stream.createTailerForPartition("consumer1", 0)) {
            Record record = tailer.read(Duration.ofMillis(1000));
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
        Streams streams = getStreams();
        String name = "foo";
        Stream stream = streams.getOrCreateStream(name, 1);

        stream.appendRecord(name, null);
        try (StreamTailer tailer = stream.createTailerForPartition("consumer1", 0)) {
            Record record = tailer.read(Duration.ofSeconds(1));
            assertNotNull(record);
            assertEquals(name, record.key);
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

    @Test
    public void testCreateAndOpen() throws Exception {
        final int partitions = 1;
        final String name = "foo";
        try (Streams streams = getStreams()) {
            Stream stream = streams.getOrCreateStream(name, partitions);
            assertEquals(name, stream.getName());
            assertEquals(partitions, stream.getPartitions());
            stream.appendRecord(Record.of("key", "value".getBytes()));
        }
        try (Streams streams = getSameStreams()) {
            Stream stream = streams.getOrCreateStream(name, 2 * partitions);
            assertEquals(name, stream.getName());
            assertEquals(partitions, stream.getPartitions());
            StreamTailer tailer = stream.createTailerForPartition("test", 0);
            Record record = tailer.read(Duration.ofSeconds(1));
            assertNotNull(record);
            assertEquals("key", record.key);
        }
    }
}
