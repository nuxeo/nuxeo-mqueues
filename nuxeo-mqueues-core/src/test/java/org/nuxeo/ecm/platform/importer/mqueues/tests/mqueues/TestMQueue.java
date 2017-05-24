/*
 * (C) Copyright 2016 Nuxeo SA (http://nuxeo.com/) and others.
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
 */
package org.nuxeo.ecm.platform.importer.mqueues.tests.mqueues;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueue;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class TestMQueue {
    protected static final Log log = LogFactory.getLog(TestMQueue.class);

    @Rule
    public
    TestName name = new TestName();

    private MQManager<IdMessage> manager;

    public abstract Duration getMinDuration();

    public abstract MQManager<IdMessage> createManager() throws Exception;

    public MQManager<IdMessage> getManager() throws Exception {
        if (manager == null) {
            manager = createManager();
        }
        return manager;
    }

    public MQueue<IdMessage> createMQ(int size) throws Exception {
        return getManager().create(name.getMethodName(), size);
    }

    public MQueue<IdMessage> reopenMQ() throws Exception {
        return getManager().open(name.getMethodName());
    }

    @After
    public void resetManager() throws Exception {
        if (manager != null) {
            manager.close();
        }
        manager = null;
    }

    @Test
    public void open() throws Exception {
        final int NB_QUEUES = 5;

        // check that the number of queues is persisted even if we don't write anything
        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUES)) {
            assertEquals(NB_QUEUES, mQueue.size());
        }
        try (MQueue<IdMessage> mQueue = reopenMQ()) {
            assertEquals(NB_QUEUES, mQueue.size());
        }

        // same with another size
        try {
            MQueue<IdMessage> mQueue = createMQ(1);
            fail("Exception should be raise because the queue already exists");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try (MQueue<IdMessage> mQueue = reopenMQ()) {
            assertEquals(NB_QUEUES, mQueue.size());
        }

    }

    @Test
    public void basicAppendAndTail() throws Exception {
        final int NB_QUEUE = 10;
        IdMessage msg1 = IdMessage.of("id1");
        IdMessage msg2 = IdMessage.of("id2");

        try (MQueue<IdMessage> mq = createMQ(NB_QUEUE)) {

            mq.append(1, msg1);

            try (MQTailer<IdMessage> tailer1 = mq.createTailer(1)) {
                assertEquals(msg1, tailer1.read(getMinDuration()));
                assertEquals(null, tailer1.read(getMinDuration()));

                mq.append(2, msg2);
                assertEquals(null, tailer1.read(getMinDuration()));

                mq.append(1, msg2);
                assertEquals(msg2, tailer1.read(getMinDuration()));
                try (MQTailer<IdMessage> tailer2 = mq.createTailer(2)) {
                    assertEquals(msg2, tailer2.read(getMinDuration()));
                }
            }
        }

        // open the mqueue offset consumer starts at the beginning because tailer have not committed.
        try (MQueue<IdMessage> mq = reopenMQ()) {
            try (MQTailer<IdMessage> tailer1 = mq.createTailer(1);
                 MQTailer<IdMessage> tailer2 = mq.createTailer(2)) {

                assertEquals(msg1, tailer1.read(getMinDuration()));
                assertEquals(msg2, tailer1.read(getMinDuration()));
                assertEquals(null, tailer1.read(getMinDuration()));

                assertEquals(msg2, tailer2.read(getMinDuration()));
                assertEquals(null, tailer2.read(getMinDuration()));
            }
        }


    }

    @Test
    public void commitOffset() throws Exception {
        final int NB_QUEUE = 10;

        // Create a queue
        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            mQueue.append(1, IdMessage.of("id1"));
            mQueue.append(1, IdMessage.of("id2"));
            mQueue.append(1, IdMessage.of("id3"));

            mQueue.append(2, IdMessage.of("id4"));
            mQueue.append(2, IdMessage.of("id5"));

            // process 2 messages and commit
            try (MQTailer<IdMessage> tailer = mQueue.createTailer(1)) {
                assertEquals("id1", tailer.read(getMinDuration()).getId());
                tailer.commit();
                //Thread.sleep(10000);
                assertEquals("id2", tailer.read(getMinDuration()).getId());
                tailer.commit();
            }

            try (MQTailer<IdMessage> tailer = mQueue.createTailer(2)) {
                assertEquals("id4", tailer.read(getMinDuration()).getId());
                tailer.commit();
                tailer.commit();
            }
        }

        // open the queue
        try (MQueue<IdMessage> mQueue = reopenMQ()) {
            try (MQTailer<IdMessage> tailer = mQueue.createTailer(1)) {
                tailer.toStart();
                assertEquals("id1", tailer.read(getMinDuration()).getId());

                tailer.toEnd();
                assertEquals(null, tailer.read(getMinDuration()));

                tailer.toLastCommitted();
                assertEquals("id3", tailer.read(getMinDuration()).getId());
            }
            // by default the tailer is open on the last committed message
            try (MQTailer<IdMessage> tailer = mQueue.createTailer(2)) {
                assertEquals("id5", tailer.read(getMinDuration()).getId());

                tailer.toStart();
                assertEquals("id4", tailer.read(getMinDuration()).getId());
            }
        }

    }

    @Test
    public void commitOffset2() throws Exception {
        final int NB_QUEUE = 10;
        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            mQueue.append(1, IdMessage.of("id1"));
            mQueue.append(1, IdMessage.of("id2"));
            mQueue.append(1, IdMessage.of("id3"));
            mQueue.append(1, IdMessage.of("id4"));

            try (MQTailer<IdMessage> tailer = mQueue.createTailer(1)) {
                assertEquals("id1", tailer.read(getMinDuration()).getId());
                tailer.commit();
                assertEquals("id2", tailer.read(getMinDuration()).getId());
                tailer.commit();

                // restart from the beginning and commit after the first message
                tailer.toStart();
                assertEquals("id1", tailer.read(getMinDuration()).getId());
                tailer.commit();
            }
        }

        // reopen
        try (MQueue<IdMessage> mQueue = reopenMQ()) {
            try (MQTailer<IdMessage> tailer = mQueue.createTailer(1)) {
                tailer.toLastCommitted();
                // the last committed message was id1
                assertEquals("id2", tailer.read(getMinDuration()).getId());
            }
        }

    }

    @Test
    public void canNotAppendOnClosedMQueue() throws Exception {
        final int NB_QUEUE = 1;

        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            mQueue.close();
            try {
                mQueue.append(0, IdMessage.of("foo"));
                fail("Can not append on closed mqueues");
            } catch (IndexOutOfBoundsException | NullPointerException e) {
                // expected
            }
        }
    }

    @Test
    public void canNotOpeningTwiceTheSameTailer() throws Exception {
        final int NB_QUEUE = 1;

        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            try (MQTailer<IdMessage> tailer = mQueue.createTailer(0)) {
                try {
                    MQTailer<IdMessage> tailerBis = mQueue.createTailer(0);
                    fail("Opening twice a tailer is not allowed");
                } catch (IllegalArgumentException e) {
                    // expected
                }

                try (MQTailer<IdMessage> tailerBis = mQueue.createTailer(0, "foo")) {
                    // with another namespace no problem
                    assertEquals(0, tailerBis.getQueue());
                    assertEquals("foo", tailerBis.getNameSpace());
                    assertEquals(mQueue.getName(), tailerBis.getMQueueName());
                    assertEquals(0, tailerBis.getQueue());
                }
            }
        }
    }

    @Test
    public void canNotOpeningTwiceTheSameTailerEvenOnDifferentMQueue() throws Exception {
        final int NB_QUEUE = 1;

        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE);
             MQueue<IdMessage> mQueueBis = reopenMQ()) {

            MQTailer<IdMessage> tailer = mQueue.createTailer(0);
            try {
                MQTailer<IdMessage> tailerBis = mQueueBis.createTailer(0);
                fail("Opening twice a tailer is not allowed");
            } catch (IllegalArgumentException e) {
                // expected
            }

            MQTailer<IdMessage> tailerBis = mQueueBis.createTailer(0, "another name space");
            try {
                MQTailer<IdMessage> tailerBisBis = mQueue.createTailer(0, "another name space");
                fail("Opening twice a tailer is not allowed");
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
    }

    @Test
    public void closingMQueueShouldCloseItsTailers() throws Exception {
        final int NB_QUEUE = 1;

        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            mQueue.append(0, IdMessage.of("foo"));
            MQTailer<IdMessage> tailer = mQueue.createTailer(0);
            assertNotNull(tailer.read(getMinDuration()));
            // here we close the mq not the tailer
        }

        try (MQueue<IdMessage> mQueue = reopenMQ()) {
            MQTailer<IdMessage> tailer = mQueue.createTailer(0);
            assertNotNull(tailer.read(getMinDuration()));
        }
    }


    @Test
    public void canNotReuseAClosedMQueue() throws Exception {
        final int NB_QUEUE = 1;

        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            mQueue.append(0, IdMessage.of("foo"));
            mQueue.close();
            try {
                mQueue.append(0, IdMessage.of("bar"));
                fail("Should raise an exception");
            } catch (IndexOutOfBoundsException | NullPointerException e) {
                // expected
            }
        }
    }

    @Test
    public void canNotReuseAClosedTailer() throws Exception {
        final int NB_QUEUE = 1;

        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            mQueue.append(0, IdMessage.of("foo"));
            MQTailer<IdMessage> tailer = mQueue.createTailer(0);
            assertNotNull(tailer.read(getMinDuration()));
            tailer.close();
            try {
                tailer.toStart();
                tailer.read(getMinDuration());
                fail("It is not possible to read on a closed tailer");
            } catch (IllegalStateException | NullPointerException e) {
                // expected
            }

        }
    }


    @Test
    public void commitOffsetNameSpace() throws Exception {
        final int NB_QUEUE = 1;
        final IdMessage msg1 = IdMessage.of("id1");
        final IdMessage msg2 = IdMessage.of("id2");

        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            for (int i = 0; i < 10; i++) {
                mQueue.append(0, IdMessage.of("id" + i));
            }
            // each tailers have distinct commit offsets
            try (MQTailer<IdMessage> tailerA = mQueue.createTailer(0, "group-a");
                 MQTailer<IdMessage> tailerB = mQueue.createTailer(0, "group-b")) {

                assertEquals("id0", tailerA.read(getMinDuration()).getId());
                assertEquals("id1", tailerA.read(getMinDuration()).getId());
                tailerA.commit();
                assertEquals("id2", tailerA.read(getMinDuration()).getId());
                assertEquals("id3", tailerA.read(getMinDuration()).getId());
                tailerA.toLastCommitted();
                assertEquals("id2", tailerA.read(getMinDuration()).getId());
                assertEquals("id3", tailerA.read(getMinDuration()).getId());


                assertEquals("id0", tailerB.read(getMinDuration()).getId());
                tailerB.commit();
                assertEquals("id1", tailerB.read(getMinDuration()).getId());
                assertEquals("id2", tailerB.read(getMinDuration()).getId());

                tailerB.toLastCommitted();
                assertEquals("id1", tailerB.read(getMinDuration()).getId());

                tailerA.toLastCommitted();
                assertEquals("id2", tailerA.read(getMinDuration()).getId());
            }
        }

        // reopen
        try (MQueue<IdMessage> mQueue = reopenMQ()) {
            try (MQTailer<IdMessage> tailer = mQueue.createTailer(0);
                 MQTailer<IdMessage> tailerA = mQueue.createTailer(0, "group-a");
                 MQTailer<IdMessage> tailerB = mQueue.createTailer(0, "group-b")) {
                assertEquals("id0", tailer.read(getMinDuration()).getId());
                assertEquals("id2", tailerA.read(getMinDuration()).getId());
                assertEquals("id1", tailerB.read(getMinDuration()).getId());
            }
        }


    }

    @Test
    public void commitConcurrentTailer() throws Exception {
        final int NB_QUEUE = 1;

        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            for (int i = 0; i < 10; i++) {
                mQueue.append(0, IdMessage.of("id" + i));
            }
            MQTailer<IdMessage> tailerA = mQueue.createTailer(0);
            MQTailer<IdMessage> tailerB = mQueue.createTailer(0, "b");

            assertEquals("id0", tailerA.read(getMinDuration()).getId());
            assertEquals("id0", tailerB.read(getMinDuration()).getId());

            assertEquals("id1", tailerA.read(getMinDuration()).getId());
            tailerA.commit();
            tailerB.commit();

            assertEquals("id1", tailerB.read(getMinDuration()).getId());
            assertEquals("id2", tailerA.read(getMinDuration()).getId());
            assertEquals("id2", tailerB.read(getMinDuration()).getId());
            assertEquals("id3", tailerB.read(getMinDuration()).getId());
            assertEquals("id4", tailerB.read(getMinDuration()).getId());
            tailerB.commit();

            tailerA.toLastCommitted();
            tailerB.toStart();
            assertEquals("id2", tailerA.read(getMinDuration()).getId());
            assertEquals("id0", tailerB.read(getMinDuration()).getId());

            tailerB.toLastCommitted();
            assertEquals("id5", tailerB.read(getMinDuration()).getId());

            tailerA.close();
            tailerB.close();
        }
    }


    @Test
    public void waitForConsumer() throws Exception {
        final int NB_QUEUE = 1;

        MQOffset offset = null;
        MQOffset offset0 = null;
        MQOffset offset5 = null;
        try (MQueue<IdMessage> mQueue = createMQ(NB_QUEUE)) {
            // appends some msg and keep some offsets
            for (int i = 0; i < 10; i++) {
                offset = mQueue.append(0, IdMessage.of("id" + i));
                if (i == 0) {
                    offset0 = offset;
                } else if (i == 5) {
                    offset5 = offset;
                }
            }
            // nothing committed
            assertFalse(mQueue.waitFor(offset, getMinDuration()));
            assertFalse(mQueue.waitFor(offset0, getMinDuration()));
            assertFalse(mQueue.waitFor(offset5, getMinDuration()));

            try (MQTailer<IdMessage> tailer = mQueue.createTailer(0)) {
                tailer.read(getMinDuration());
                tailer.commit();

                // msg 0 is processed and committed
                assertTrue(mQueue.waitFor(offset0, getMinDuration()));
                // msg 5 and last is processed and committed
                assertFalse(mQueue.waitFor(offset5, getMinDuration()));
                assertFalse(mQueue.waitFor(offset, getMinDuration()));

                // drain
                while (tailer.read(getMinDuration()) != null) ;

                // message is processed but not yet committed
                assertFalse(mQueue.waitFor(offset, getMinDuration()));
                tailer.commit();
            }

            // message is processed and committed
            assertTrue(mQueue.waitFor(offset0, getMinDuration()));
            assertTrue(mQueue.waitFor(offset5, getMinDuration()));
            assertTrue(mQueue.waitFor(offset, getMinDuration()));

        }
    }


    @Test
    public void testMQManager() throws Exception {
        MQManager<IdMessage> qman = getManager();
        String name1 = "foo";
        MQueue<IdMessage> stream = qman.openOrCreate(name1, 10);
        assertEquals(name1, stream.getName());
        assertEquals(10, stream.size());

        String name2 = "bar";
        MQueue<IdMessage> stream2 = qman.openOrCreate(name2, 5);
        MQueue<IdMessage> stream3 = qman.get(name2);
        assertEquals(stream2, stream3);
        stream3 = qman.openOrCreate(name2, 5);
        assertEquals(stream2, stream3);

        assertNull(qman.get("unknown"));
    }

    @Test
    public void testMQManagerCreateAndOpen() throws Exception {
        final int partitions = 1;
        final String name = "foo";
        try (MQManager<IdMessage> qman = getManager()) {
            MQueue<IdMessage> mqueue = qman.openOrCreate(name, partitions);
            assertEquals(name, mqueue.getName());
            assertEquals(partitions, mqueue.size());
            mqueue.append("key", IdMessage.of("key", "value".getBytes()));


            mqueue = qman.openOrCreate(name, 2 * partitions);
            assertEquals(name, mqueue.getName());
            // the new size is not taken in account because the queue exists
            assertEquals(partitions, mqueue.size());

            MQTailer<IdMessage> tailer = mqueue.createTailer(0, "test");
            IdMessage message = tailer.read(Duration.ofSeconds(1));
            assertNotNull(message);
            assertEquals("key", message.getId());
        }
    }

}
