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
package org.nuxeo.ecm.platform.importer.mqueues.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.nuxeo.ecm.platform.importer.mqueues.message.IdMessage;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.MQueues;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.Offset;
import org.nuxeo.ecm.platform.importer.mqueues.mqueues.chronicles.CQMQueues;

import java.io.File;
import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCQMQueues {

    protected static final Log log = LogFactory.getLog(TestCQMQueues.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public CQMQueues<IdMessage> createMQ(File basePath, int NB_QUEUES) {
        return new CQMQueues<>(basePath, NB_QUEUES);
    }

    public CQMQueues<IdMessage> openMQ(File basePath) {
        return new CQMQueues<>(basePath);
    }


    @Test
    public void open() throws Exception {
        final File basePath = folder.newFolder("cq");
        final int NB_QUEUES = 5;

        // check that the number of queues is persisted even if we don't write anything
        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUES)) {
            assertEquals(NB_QUEUES, mQueues.size());
        }
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            assertEquals(NB_QUEUES, mQueues.size());
        }

        // same with another size
        try (MQueues<IdMessage> mQueues = createMQ(basePath, 1)) {
            assertEquals(1, mQueues.size());
        }
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            assertEquals(1, mQueues.size());
        }

        // add a file in the basePath
        File aFile = new File(basePath, "foo.txt");
        aFile.createNewFile();

        // create a new mqueues but fails because it does not looks like a mqueues
        try (MQueues<IdMessage> mQueues = createMQ(basePath, 1)) {
            fail("Create a mqueue on an existing folder with extra data is not allowed");
        } catch (IllegalArgumentException e) {
            // expected
        }


    }

    @Test
    public void basicAppendAndTail() throws Exception {
        final int NB_QUEUE = 10;
        IdMessage msg1 = IdMessage.of("id1");
        IdMessage msg2 = IdMessage.of("id2");
        File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mq = createMQ(basePath, NB_QUEUE)) {

            mq.append(1, msg1);

            try (MQueues.Tailer<IdMessage> tailer1 = mq.createTailer(1)) {
                assertEquals(msg1, tailer1.read(Duration.ofMillis(1)));
                assertEquals(null, tailer1.read(Duration.ofMillis(1)));

                mq.append(2, msg2);
                assertEquals(null, tailer1.read(Duration.ofMillis(1)));

                mq.append(1, msg2);
                assertEquals(msg2, tailer1.read(Duration.ofMillis(1)));
                try (MQueues.Tailer<IdMessage> tailer2 = mq.createTailer(2)) {
                    assertEquals(msg2, tailer2.read(Duration.ofMillis(1)));
                }
            }
        }

        // open the mqueue offset consumer starts at the beginning because tailer have not committed.
        try (MQueues<IdMessage> mq = openMQ(basePath)) {
            try (MQueues.Tailer<IdMessage> tailer1 = mq.createTailer(1);
                 MQueues.Tailer<IdMessage> tailer2 = mq.createTailer(2)) {

                assertEquals(msg1, tailer1.read(Duration.ofMillis(1)));
                assertEquals(msg2, tailer1.read(Duration.ofMillis(1)));
                assertEquals(null, tailer1.read(Duration.ofMillis(1)));

                assertEquals(msg2, tailer2.read(Duration.ofMillis(1)));
                assertEquals(null, tailer2.read(Duration.ofMillis(1)));
            }
        }


    }

    @Test
    public void commitOffset() throws Exception {
        final int NB_QUEUE = 10;
        final File basePath = folder.newFolder("cq");

        // Create a queue
        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            mQueues.append(1, IdMessage.of("id1"));
            mQueues.append(1, IdMessage.of("id2"));
            mQueues.append(1, IdMessage.of("id3"));

            mQueues.append(2, IdMessage.of("id4"));
            mQueues.append(2, IdMessage.of("id5"));

            // process 2 messages and commit
            try (MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(1)) {
                assertEquals("id1", tailer.read(Duration.ofMillis(0)).getId());
                tailer.commit();
                assertEquals("id2", tailer.read(Duration.ofMillis(0)).getId());
                tailer.commit();
            }

            try (MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(2)) {
                assertEquals("id4", tailer.read(Duration.ofMillis(0)).getId());
                tailer.commit();
                tailer.commit();
            }
        }

        // open the queue
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            try (MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(1)) {
                tailer.toStart();
                assertEquals("id1", tailer.read(Duration.ofMillis(0)).getId());

                tailer.toEnd();
                assertEquals(null, tailer.read(Duration.ofMillis(0)));

                tailer.toLastCommitted();
                assertEquals("id3", tailer.read(Duration.ofMillis(0)).getId());
            }
            // by default the tailer is open on the last committed message
            try (MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(2)) {
                assertEquals("id5", tailer.read(Duration.ofMillis(0)).getId());

                tailer.toStart();
                assertEquals("id4", tailer.read(Duration.ofMillis(0)).getId());
            }
        }

    }

    @Test
    public void commitOffset2() throws Exception {
        final int NB_QUEUE = 10;
        final File basePath = folder.newFolder("cq");
        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            mQueues.append(1, IdMessage.of("id1"));
            mQueues.append(1, IdMessage.of("id2"));
            mQueues.append(1, IdMessage.of("id3"));
            mQueues.append(1, IdMessage.of("id4"));

            try (MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(1)) {
                assertEquals("id1", tailer.read(Duration.ofMillis(1)).getId());
                tailer.commit();
                assertEquals("id2", tailer.read(Duration.ofMillis(1)).getId());
                tailer.commit();

                // restart from the beginning and commit after the first message
                tailer.toStart();
                assertEquals("id1", tailer.read(Duration.ofMillis(1)).getId());
                tailer.commit();
            }
        }

        // reopen
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            try (MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(1)) {
                tailer.toLastCommitted();
                // the last committed message was id1
                assertEquals("id2", tailer.read(Duration.ofMillis(1)).getId());
            }
        }

    }

    @Test
    public void canNotAppendOnClosedMQueues() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            mQueues.close();
            try {
                mQueues.append(0, IdMessage.of("foo"));
                fail("Can not append on closed mqueues");
            } catch (IndexOutOfBoundsException e) {
                // expected
            }
        }
    }

    @Test
    public void canNotOpeningTwiceTheSameTailer() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            try (MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0)) {
                try {
                    MQueues.Tailer<IdMessage> tailerBis = mQueues.createTailer(0);
                    fail("Opening twice a tailer is not allowed");
                } catch (IllegalArgumentException e) {
                    // expected
                }

                try (MQueues.Tailer<IdMessage> tailerBis = mQueues.createTailer(0, "foo")) {
                    // with another namespace no problem
                    assertEquals(0, tailerBis.getQueue());
                }
            }
        }
    }

    @Test
    public void canNotOpeningTwiceTheSameTailerEvenOnDifferentMQueues() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE);
             MQueues<IdMessage> mQueuesBis = openMQ(basePath)) {

            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0);
            try {
                MQueues.Tailer<IdMessage> tailerBis = mQueuesBis.createTailer(0);
                fail("Opening twice a tailer is not allowed");
            } catch (IllegalArgumentException e) {
                // expected
            }

            MQueues.Tailer<IdMessage> tailerBis = mQueuesBis.createTailer(0, "another name space");
            try {
                MQueues.Tailer<IdMessage> tailerBisBis = mQueues.createTailer(0, "another name space");
                fail("Opening twice a tailer is not allowed");
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
    }

    @Test
    public void closingMQueuesShouldCloseItsTailers() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            mQueues.append(0, IdMessage.of("foo"));
            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0);
            assertNotNull(tailer.read(Duration.ofMillis(1)));
            // here we close the mq not the tailer
        }

        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0);
            assertNotNull(tailer.read(Duration.ofMillis(1)));
        }
    }


    @Test
    public void canNotReuseAClosedMQueues() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            mQueues.append(0, IdMessage.of("foo"));
            mQueues.close();
            try {
                mQueues.append(0, IdMessage.of("bar"));
                fail("Should raise an exception");
            } catch (IndexOutOfBoundsException e) {
                // expected
            }
        }
    }

    @Test
    public void canNotReuseAClosedTailer() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            mQueues.append(0, IdMessage.of("foo"));
            MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0);
            assertNotNull(tailer.read(Duration.ofMillis(0)));
            tailer.close();
            tailer.toStart();
            try {
                tailer.read(Duration.ofMillis(0));
                fail("It is not possible to read on a closed tailer");
            } catch (IllegalStateException e) {
                // expected
            }

        }
    }


    @Test
    public void commitOffsetNameSpace() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");
        final IdMessage msg1 = IdMessage.of("id1");
        final IdMessage msg2 = IdMessage.of("id2");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            for (int i = 0; i < 10; i++) {
                mQueues.append(0, IdMessage.of("id" + i));
            }
            // each tailers have distinct commit offsets
            try (MQueues.Tailer<IdMessage> tailerA = mQueues.createTailer(0, "a");
                 MQueues.Tailer<IdMessage> tailerB = mQueues.createTailer(0, "b")) {

                assertEquals("id0", tailerA.read(Duration.ofMillis(0)).getId());
                assertEquals("id1", tailerA.read(Duration.ofMillis(0)).getId());
                tailerA.commit();
                assertEquals("id2", tailerA.read(Duration.ofMillis(0)).getId());
                assertEquals("id3", tailerA.read(Duration.ofMillis(0)).getId());
                tailerA.toLastCommitted();
                assertEquals("id2", tailerA.read(Duration.ofMillis(0)).getId());
                assertEquals("id3", tailerA.read(Duration.ofMillis(0)).getId());


                assertEquals("id0", tailerB.read(Duration.ofMillis(0)).getId());
                tailerB.commit();
                assertEquals("id1", tailerB.read(Duration.ofMillis(0)).getId());
                assertEquals("id2", tailerB.read(Duration.ofMillis(0)).getId());

                tailerB.toLastCommitted();
                assertEquals("id1", tailerB.read(Duration.ofMillis(0)).getId());

                tailerA.toLastCommitted();
                assertEquals("id2", tailerA.read(Duration.ofMillis(0)).getId());
            }
        }

        // reopen
        try (MQueues<IdMessage> mQueues = openMQ(basePath)) {
            try (MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0);
                 MQueues.Tailer<IdMessage> tailerA = mQueues.createTailer(0, "a");
                 MQueues.Tailer<IdMessage> tailerB = mQueues.createTailer(0, "b")) {
                assertEquals("id0", tailer.read(Duration.ofMillis(0)).getId());
                assertEquals("id2", tailerA.read(Duration.ofMillis(0)).getId());
                assertEquals("id1", tailerB.read(Duration.ofMillis(0)).getId());
            }
        }


    }

    @Test
    public void commitConcurrentTailer() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            for (int i = 0; i < 10; i++) {
                mQueues.append(0, IdMessage.of("id" + i));
            }
            MQueues.Tailer<IdMessage> tailerA = mQueues.createTailer(0);
            MQueues.Tailer<IdMessage> tailerB = mQueues.createTailer(0, "foo");

            assertEquals("id0", tailerA.read(Duration.ofMillis(0)).getId());
            assertEquals("id0", tailerB.read(Duration.ofMillis(0)).getId());

            assertEquals("id1", tailerA.read(Duration.ofMillis(0)).getId());
            tailerA.commit();
            tailerB.commit();

            assertEquals("id1", tailerB.read(Duration.ofMillis(0)).getId());
            assertEquals("id2", tailerA.read(Duration.ofMillis(0)).getId());
            assertEquals("id2", tailerB.read(Duration.ofMillis(0)).getId());
            assertEquals("id3", tailerB.read(Duration.ofMillis(0)).getId());
            assertEquals("id4", tailerB.read(Duration.ofMillis(0)).getId());
            tailerB.commit();

            tailerA.toLastCommitted();
            tailerB.toStart();
            assertEquals("id2", tailerA.read(Duration.ofMillis(0)).getId());
            assertEquals("id0", tailerB.read(Duration.ofMillis(0)).getId());

            tailerB.toLastCommitted();
            assertEquals("id5", tailerB.read(Duration.ofMillis(0)).getId());

            tailerA.close();
            tailerB.close();
        }
    }


    @Test
    public void waitForConsumer() throws Exception {
        final int NB_QUEUE = 1;
        final File basePath = folder.newFolder("cq");

        Offset offset = null;
        Offset offset0 = null;
        Offset offset5 = null;
        try (MQueues<IdMessage> mQueues = createMQ(basePath, NB_QUEUE)) {
            // appends some msg and keep some offsets
            for (int i = 0; i < 10; i++) {
                offset = mQueues.append(0, IdMessage.of("id" + i));
                if (i == 0) {
                    offset0 = offset;
                } else if (i == 5) {
                    offset5 = offset;
                }
            }
            // nothing committed
            assertFalse(mQueues.waitFor(offset, Duration.ofMillis(0)));
            assertFalse(mQueues.waitFor(offset0, Duration.ofMillis(0)));
            assertFalse(mQueues.waitFor(offset5, Duration.ofMillis(0)));

            try (MQueues.Tailer<IdMessage> tailer = mQueues.createTailer(0)) {
                tailer.read(Duration.ofMillis(0));
                tailer.commit();

                // msg 0 is processed and committed
                assertTrue(mQueues.waitFor(offset0, Duration.ofMillis(0)));
                // msg 5 and last is processed and committed
                assertFalse(mQueues.waitFor(offset5, Duration.ofMillis(0)));
                assertFalse(mQueues.waitFor(offset, Duration.ofMillis(0)));

                // drain
                while (tailer.read(Duration.ofMillis(0)) != null) ;

                // message is processed but not yet committed
                assertFalse(mQueues.waitFor(offset, Duration.ofMillis(0)));
                tailer.commit();
            }

            // message is processed and committed
            assertTrue(mQueues.waitFor(offset0, Duration.ofMillis(0)));
            assertTrue(mQueues.waitFor(offset5, Duration.ofMillis(0)));
            assertTrue(mQueues.waitFor(offset, Duration.ofMillis(0)));

        }
    }
}
