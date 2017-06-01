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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQAppender;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQManager;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQOffset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQPartition;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQTailer;
import org.nuxeo.ecm.platform.importer.mqueues.pattern.IdMessage;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class TestMQueue {
    protected static final Log log = LogFactory.getLog(TestMQueue.class);
    protected String mqName = "mqName";

    @Rule
    public
    TestName name = new TestName();

    private MQManager<IdMessage> manager;

    public abstract Duration getMinDuration();

    public abstract MQManager<IdMessage> createManager() throws Exception;

    @Before
    public void initManager() throws Exception {
        mqName = name.getMethodName();
        if (manager == null) {
            manager = createManager();
        }
    }

    @After
    public void closeManager() throws Exception {
        if (manager != null) {
            manager.close();
        }
        manager = null;
    }

    public void resetManager() throws Exception {
        closeManager();
        initManager();
    }

    @Test
    public void open() throws Exception {
        final int NB_QUEUES = 5;
        String mqName = name.getMethodName();

        // check that the number of queues is persisted even if we don't write anything
        manager.createIfNotExists(mqName, NB_QUEUES);
        assertEquals(NB_QUEUES, manager.getAppender(mqName).size());
        // reset the manager
        resetManager();
        assertEquals(NB_QUEUES, manager.getAppender(mqName).size());

        // with a different size
        resetManager();
        manager.createIfNotExists(mqName, 1);
        assertEquals(NB_QUEUES, manager.getAppender(mqName).size());
    }



    @Test
    public void basicAppendAndTail() throws Exception {
        final int NB_QUEUE = 10;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);

        IdMessage msg1 = IdMessage.of("id1");
        IdMessage msg2 = IdMessage.of("id2");
        appender.append(1, msg1);

        try (MQTailer<IdMessage> tailer1 = manager.createTailer(MQPartition.of(mqName, 1), "default")) {
            assertEquals(msg1, tailer1.read(getMinDuration()));
            assertEquals(null, tailer1.read(getMinDuration()));

            appender.append(2, msg2);
            assertEquals(null, tailer1.read(getMinDuration()));

            appender.append(1, msg2);
            assertEquals(msg2, tailer1.read(getMinDuration()));
        }

        try (MQTailer<IdMessage> tailer2 = manager.createTailer(MQPartition.of(mqName, 2), "default")) {
            assertEquals(msg2, tailer2.read(getMinDuration()));
        }

        // open the mqueue offset consumer starts at the beginning because tailer have not committed.
        try (MQTailer<IdMessage> tailer1 = manager.createTailer(MQPartition.of(mqName, 1), "default");
             MQTailer<IdMessage> tailer2 = manager.createTailer(MQPartition.of(mqName, 2), "default")) {
            assertEquals(msg1, tailer1.read(getMinDuration()));
            assertEquals(msg2, tailer1.read(getMinDuration()));
            assertEquals(null, tailer1.read(getMinDuration()));

            assertEquals(msg2, tailer2.read(getMinDuration()));
            assertEquals(null, tailer2.read(getMinDuration()));
        }
    }


    @Test
    public void commitOffset() throws Exception {
        final int NB_QUEUE = 10;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);

        // Create a queue
        appender.append(1, IdMessage.of("id1"));
        appender.append(1, IdMessage.of("id2"));
        appender.append(1, IdMessage.of("id3"));

        appender.append(2, IdMessage.of("id4"));
        appender.append(2, IdMessage.of("id5"));

        // process 2 messages and commit
        try (MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 1), "default")) {
            assertEquals("id1", tailer.read(getMinDuration()).getId());
            tailer.commit();
            //Thread.sleep(10000);
            assertEquals("id2", tailer.read(getMinDuration()).getId());
            tailer.commit();
        }

        try (MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 2), "default")) {
            assertEquals("id4", tailer.read(getMinDuration()).getId());
            tailer.commit();
            tailer.commit();
        }

        resetManager();

        try (MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 1), "default")) {
            tailer.toStart();
            assertEquals("id1", tailer.read(getMinDuration()).getId());

            tailer.toEnd();
            assertEquals(null, tailer.read(getMinDuration()));

            tailer.toLastCommitted();
            assertEquals("id3", tailer.read(getMinDuration()).getId());
        }
        // by default the tailer is open on the last committed message
        try (MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 2), "default")) {
            assertEquals("id5", tailer.read(getMinDuration()).getId());

            tailer.toStart();
            assertEquals("id4", tailer.read(getMinDuration()).getId());
        }
    }


    @Test
    public void commitOffset2() throws Exception {
        final int NB_QUEUE = 10;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);


        appender.append(1, IdMessage.of("id1"));
        appender.append(1, IdMessage.of("id2"));
        appender.append(1, IdMessage.of("id3"));
        appender.append(1, IdMessage.of("id4"));

        try (MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 1), "default")) {
            assertEquals("id1", tailer.read(getMinDuration()).getId());
            tailer.commit();
            assertEquals("id2", tailer.read(getMinDuration()).getId());
            tailer.commit();

            // restart from the beginning and commit after the first message
            tailer.toStart();
            assertEquals("id1", tailer.read(getMinDuration()).getId());
            tailer.commit();
        }

        // reopen
        try (MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 1), "default")) {
            tailer.toLastCommitted();
            // the last committed message was id1
            assertEquals("id2", tailer.read(getMinDuration()).getId());
        }

    }

    @Test
    public void canNotAppendOnClosedMQueue() throws Exception {
        final int NB_QUEUE = 1;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);
        appender.close();
        try {
            appender.append(0, IdMessage.of("foo"));
            fail("Can not append on closed mqueues");
        } catch (IndexOutOfBoundsException | NullPointerException e) {
            // expected
        }
    }


    @Test
    public void canNotOpeningTwiceTheSameTailer() throws Exception {
        final int NB_QUEUE = 1;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);

        try (MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 0), "default")) {
            try {
                MQTailer<IdMessage> tailerDuplicate = manager.createTailer(MQPartition.of(mqName, 0), "default");
                fail("Opening twice a tailer is not allowed");
            } catch (IllegalArgumentException e) {
                // expected
            }

            try (MQTailer<IdMessage> tailerBis = manager.createTailer(MQPartition.of(mqName, 0), "anotherGroup")) {
                // with another namespace no problem
                assertEquals(0, tailerBis.getQueue());
                assertEquals("anotherGroup", tailerBis.getGroup());
                assertEquals(appender.getName(), tailerBis.getMQPartition().name());
                assertEquals(0, tailerBis.getMQPartition().partition());
                assertEquals(0, tailerBis.getQueue());
            }
        }
    }

    @Test
    public void closingManagerShouldCloseItsTailers() throws Exception {
        final int NB_QUEUE = 1;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);
        appender.append(0, IdMessage.of("foo"));

        MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 0), "default");
        assertNotNull(tailer.read(getMinDuration()));

        resetManager();

        try {
            tailer.read(getMinDuration());
            fail("Can not read from a closed tailer");
        } catch (IllegalStateException | NullPointerException e) {
            // expected
        }
    }


    @Test
    public void canNotReuseAClosedTailer() throws Exception {
        final int NB_QUEUE = 1;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);
        appender.append(0, IdMessage.of("foo"));
        MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 0), "default");
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


    @Test
    public void commitOffsetNameSpace() throws Exception {
        final int NB_QUEUE = 1;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);

        final IdMessage msg1 = IdMessage.of("id1");
        final IdMessage msg2 = IdMessage.of("id2");

        for (int i = 0; i < 10; i++) {
            appender.append(0, IdMessage.of("id" + i));
        }
        // each tailers have distinct commit offsets
        try (MQTailer<IdMessage> tailerA = manager.createTailer(MQPartition.of(mqName, 0), "group-a");
             MQTailer<IdMessage> tailerB = manager.createTailer(MQPartition.of(mqName, 0), "group-b")) {

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


        // reopen
        resetManager();

        try (MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 0), "default");
             MQTailer<IdMessage> tailerA = manager.createTailer(MQPartition.of(mqName, 0), "group-a");
             MQTailer<IdMessage> tailerB = manager.createTailer(MQPartition.of(mqName, 0), "group-b")) {
            assertEquals("id0", tailer.read(getMinDuration()).getId());
            assertEquals("id2", tailerA.read(getMinDuration()).getId());
            assertEquals("id1", tailerB.read(getMinDuration()).getId());
        }
    }

    @Test
    public void commitConcurrentTailer() throws Exception {
        final int NB_QUEUE = 1;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);

        for (int i = 0; i < 10; i++) {
            appender.append(0, IdMessage.of("id" + i));
        }
        MQTailer<IdMessage> tailerA = manager.createTailer(MQPartition.of(mqName, 0), "group-a");
        MQTailer<IdMessage> tailerB = manager.createTailer(MQPartition.of(mqName, 0), "group-b");

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


    @Test
    public void waitForConsumer() throws Exception {
        final int NB_QUEUE = 1;
        manager.createIfNotExists(mqName, NB_QUEUE);
        MQAppender<IdMessage> appender = manager.getAppender(mqName);

        MQOffset offset = null;
        MQOffset offset0 = null;
        MQOffset offset5 = null;
        // appends some msg and keep some offsets
        for (int i = 0; i < 10; i++) {
            offset = appender.append(0, IdMessage.of("id" + i));
            if (i == 0) {
                offset0 = offset;
            } else if (i == 5) {
                offset5 = offset;
            }
        }
        // nothing committed
        assertFalse(appender.waitFor(offset, "foo", getMinDuration()));
        assertFalse(appender.waitFor(offset0, "foo", getMinDuration()));
        assertFalse(appender.waitFor(offset5, "foo", getMinDuration()));

        String group = "default";
        try (MQTailer<IdMessage> tailer = manager.createTailer(MQPartition.of(mqName, 0), group)) {
            tailer.read(getMinDuration());
            tailer.commit();

            // msg 0 is processed and committed
            assertTrue(appender.waitFor(offset0, group, getMinDuration()));
            // msg 5 and last is processed and committed
            assertFalse(appender.waitFor(offset5, group, getMinDuration()));
            assertFalse(appender.waitFor(offset, group, getMinDuration()));

            // drain
            while (tailer.read(getMinDuration()) != null) ;

            // message is processed but not yet committed
            assertFalse(appender.waitFor(offset, group, getMinDuration()));
            tailer.commit();
        }

        // message is processed and committed
        assertTrue(appender.waitFor(offset0, group, getMinDuration()));
        assertTrue(appender.waitFor(offset5, group, getMinDuration()));
        assertTrue(appender.waitFor(offset, group, getMinDuration()));

    }


    @Test
    public void testMQManager() throws Exception {
        String name1 = "foo";
        assertFalse(manager.exists(name1));
        assertTrue(manager.createIfNotExists(name1, 10));
        assertFalse(manager.createIfNotExists(name1, 10));
        assertTrue(manager.exists(name1));

        assertEquals(name1, manager.getAppender(name1).getName());
        assertEquals(10, manager.getAppender(name1).size());


        assertEquals(name1, manager.createTailer(MQPartition.of(name1, 0), "default").getMQPartition().name());
        assertEquals(1,  manager.createTailer(MQPartition.of(name1, 1), "default").getMQPartition().partition());

        String name2 = "bar";
        manager.createIfNotExists(name2, 5);

        assertFalse(manager.exists("unknown"));
        try {
            manager.getAppender("unknown");
            fail("Should have raise an exception");
        } catch (IllegalArgumentException e) {
            // expected invalid queue name
        }
        try {
            manager.createTailer(MQPartition.of("unknown", 0), "default");
            fail("Should have raise an exception");
        } catch (IllegalArgumentException e) {
            // expected invalid queue name
        }

        try {
            manager.createTailer(MQPartition.of(name1, 100), "default");
            fail("Should have raise an exception");
        } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
            // expected invalid partition
        }

    }

    @Test
    public void testMQManagerCreateAndOpen() throws Exception {
        final int partitions = 1;
        final String name = "foo";
        assertTrue(manager.createIfNotExists(name, partitions));
        manager.getAppender(name).append("key", IdMessage.of("key", "value".getBytes()));

        IdMessage message = manager.createTailer(MQPartition.of(name, 0), "test").read(Duration.ofSeconds(1));
        assertNotNull(message);
        assertEquals("key", message.getId());

    }

}
